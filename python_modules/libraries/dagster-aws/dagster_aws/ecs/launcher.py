import warnings
from collections import namedtuple
from contextlib import suppress

import boto3
from botocore.exceptions import ClientError

from dagster import Array, Field, Noneable, ScalarUnion, StringSource, check
from dagster.core.events import EngineEventData, MetadataEntry
from dagster.core.launcher.base import LaunchRunContext, RunLauncher
from dagster.grpc.types import ExecuteRunArgs
from dagster.serdes import ConfigurableClass

from ..secretsmanager import get_secrets_from_arns
from .container_context import EcsContainerContext
from .tasks import default_ecs_task_definition, default_ecs_task_metadata
from .utils import sanitize_family

Tags = namedtuple("Tags", ["arn", "cluster", "cpu", "memory"])


class EcsRunLauncher(RunLauncher, ConfigurableClass):
    """RunLauncher that starts a task in ECS for each Dagster job run."""

    def __init__(
        self,
        inst_data=None,
        task_definition=None,
        container_name="run",
        secrets=None,
        secrets_tag="dagster",
        include_sidecars=False,
    ):
        self._inst_data = inst_data
        self.ecs = boto3.client("ecs")
        self.ec2 = boto3.resource("ec2")
        self.secrets_manager = boto3.client("secretsmanager")

        self.task_definition = task_definition
        self.container_name = container_name

        self.secrets = check.opt_list_param(secrets, "secrets")

        if self.secrets and all(isinstance(secret, str) for secret in self.secrets):
            warnings.warn(
                "Setting secrets as a list of ARNs is deprecated. "
                "Secrets should instead follow the same structure as the ECS API: "
                "https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_Secret.html",
                DeprecationWarning,
            )
            self.secrets = [
                {"name": name, "valueFrom": value_from}
                for name, value_from in get_secrets_from_arns(
                    self.secrets_manager, self.secrets
                ).items()
            ]

        self.secrets_tags = [secrets_tag] if secrets_tag else []
        self.include_sidecars = include_sidecars

        if self.task_definition:
            task_definition = self.ecs.describe_task_definition(taskDefinition=task_definition)
            container_names = [
                container.get("name")
                for container in task_definition["taskDefinition"]["containerDefinitions"]
            ]
            check.invariant(
                container_name in container_names,
                f"Cannot override container '{container_name}' in task definition "
                f"'{self.task_definition}' because the container is not defined.",
            )
            self.task_definition = task_definition["taskDefinition"]["taskDefinitionArn"]

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "task_definition": Field(
                StringSource,
                is_required=False,
                description=(
                    "The task definition to use when launching new tasks. "
                    "If none is provided, each run will create its own task "
                    "definition."
                ),
            ),
            "container_name": Field(
                StringSource,
                is_required=False,
                default_value="run",
                description=(
                    "The container name to use when launching new tasks. Defaults to 'run'."
                ),
            ),
            "secrets": Field(
                Array(
                    ScalarUnion(
                        scalar_type=str,
                        non_scalar_schema={"name": StringSource, "valueFrom": StringSource},
                    )
                ),
                is_required=False,
                description=(
                    "An array of AWS Secrets Manager secrets. These secrets will "
                    "be mounted as environment variabls in the container. See "
                    "https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_Secret.html."
                ),
            ),
            "secrets_tag": Field(
                Noneable(StringSource),
                is_required=False,
                default_value="dagster",
                description=(
                    "AWS Secrets Manager secrets with this tag will be mounted as "
                    "environment variables in the container. Defaults to 'dagster'."
                ),
            ),
            "include_sidecars": Field(
                bool,
                is_required=False,
                default_value=False,
                description=(
                    "Whether each run should use the same sidecars as the task that launches it. "
                    "Defaults to False."
                ),
            ),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return EcsRunLauncher(inst_data=inst_data, **config_value)

    def _set_ecs_tags(self, run_id, task_arn):
        try:
            tags = [{"key": "dagster/run_id", "value": run_id}]
            self.ecs.tag_resource(resourceArn=task_arn, tags=tags)
        except ClientError:
            pass

    def _set_run_tags(self, run_id, task_arn):
        cluster = self._task_metadata().cluster
        tags = {"ecs/task_arn": task_arn, "ecs/cluster": cluster}
        self._instance.add_run_tags(run_id, tags)

    def _get_run_tags(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        tags = run.tags if run else {}
        arn = tags.get("ecs/task_arn")
        cluster = tags.get("ecs/cluster")
        cpu = tags.get("ecs/cpu")
        memory = tags.get("ecs/memory")

        return Tags(arn, cluster, cpu, memory)

    def launch_run(self, context: LaunchRunContext) -> None:

        """
        Launch a run in an ECS task.

        Currently, Fargate is the only supported launchType and awsvpc is the
        only supported networkMode. These are the defaults that are set up by
        docker-compose when you use the Dagster ECS reference deployment.
        """
        run = context.pipeline_run
        family = sanitize_family(
            run.external_pipeline_origin.external_repository_origin.repository_location_origin.location_name
        )

        container_context = EcsContainerContext.create_for_run(run, self)

        metadata = self._task_metadata()
        pipeline_origin = context.pipeline_code_origin
        image = pipeline_origin.repository_origin.container_image
        task_definition = self._task_definition(family, metadata, image, container_context)[
            "family"
        ]

        args = ExecuteRunArgs(
            pipeline_origin=pipeline_origin,
            pipeline_run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        )
        command = args.get_command_args()

        # Set cpu or memory overrides
        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
        cpu_and_memory_overrides = {}
        tags = self._get_run_tags(run.run_id)
        if tags.cpu:
            cpu_and_memory_overrides["cpu"] = tags.cpu
        if tags.memory:
            cpu_and_memory_overrides["memory"] = tags.memory

        # Run a task using the same network configuration as this processes's
        # task.
        response = self.ecs.run_task(
            taskDefinition=task_definition,
            cluster=metadata.cluster,
            overrides={
                "containerOverrides": [
                    {
                        "name": self.container_name,
                        "command": command,
                        # containerOverrides expects cpu/memory as integers
                        **{k: int(v) for k, v in cpu_and_memory_overrides.items()},
                    }
                ],
                # taskOverrides expects cpu/memory as strings
                **cpu_and_memory_overrides,
            },
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": metadata.subnets,
                    "assignPublicIp": metadata.assign_public_ip,
                    "securityGroups": metadata.security_groups,
                }
            },
            launchType="FARGATE",
        )

        tasks = response["tasks"]

        if not tasks:
            failures = response["failures"]
            exceptions = []
            for failure in failures:
                arn = failure.get("arn")
                reason = failure.get("reason")
                detail = failure.get("detail")
                exceptions.append(Exception(f"Task {arn} failed because {reason}: {detail}"))
            raise Exception(exceptions)

        arn = tasks[0]["taskArn"]
        self._set_run_tags(run.run_id, task_arn=arn)
        self._set_ecs_tags(run.run_id, task_arn=arn)
        self._instance.report_engine_event(
            message="Launching run in ECS task",
            pipeline_run=run,
            engine_event_data=EngineEventData(
                [
                    MetadataEntry("ECS Task ARN", value=arn),
                    MetadataEntry("ECS Cluster", value=metadata.cluster),
                    MetadataEntry("Run ID", value=run.run_id),
                ]
            ),
            cls=self.__class__,
        )

    def can_terminate(self, run_id):
        tags = self._get_run_tags(run_id)

        if not (tags.arn and tags.cluster):
            return False

        tasks = self.ecs.describe_tasks(tasks=[tags.arn], cluster=tags.cluster).get("tasks")
        if not tasks:
            return False

        status = tasks[0].get("lastStatus")
        if status and status != "STOPPED":
            return True

        return False

    def terminate(self, run_id):
        tags = self._get_run_tags(run_id)

        if not (tags.arn and tags.cluster):
            return False

        tasks = self.ecs.describe_tasks(tasks=[tags.arn], cluster=tags.cluster).get("tasks")
        if not tasks:
            return False

        status = tasks[0].get("lastStatus")
        if status == "STOPPED":
            return False

        self.ecs.stop_task(task=tags.arn, cluster=tags.cluster)
        return True

    def _task_definition(self, family, metadata, image, container_context):
        """
        Return the launcher's task definition if it's configured.

        Otherwise, a new task definition revision is registered for every run.
        First, the process that calls this method finds its own task
        definition. Next, it creates a new task definition based on its own
        but it overrides the image with the pipeline origin's image.
        """
        if self.task_definition:
            task_definition = self.ecs.describe_task_definition(taskDefinition=self.task_definition)
            return task_definition["taskDefinition"]

        secrets = container_context.get_secrets_dict(self.secrets_manager)
        secrets_definition = (
            {"secrets": [{"name": key, "valueFrom": value} for key, value in secrets.items()]}
            if secrets
            else {}
        )

        task_definition = {}
        with suppress(ClientError):
            task_definition = self.ecs.describe_task_definition(taskDefinition=family)[
                "taskDefinition"
            ]

        container_definitions = task_definition.get("containerDefinitions", [{}])
        for container_definition in container_definitions:
            if (
                container_definition.get("image") == image
                and container_definition.get("name") == self.container_name
                and container_definition.get("secrets") == secrets_definition.get("secrets", [])
            ):
                return task_definition

        return default_ecs_task_definition(
            self.ecs,
            family,
            metadata,
            image,
            self.container_name,
            secrets=secrets_definition,
            include_sidecars=self.include_sidecars,
        )

    def _task_metadata(self):
        return default_ecs_task_metadata(self.ec2, self.ecs)
