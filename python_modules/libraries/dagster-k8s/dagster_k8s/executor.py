import kubernetes
from dagster_k8s.launcher import K8sRunLauncher

from dagster import Field, StringSource, check, executor
from dagster.core.definitions.executor_definition import multiple_process_executor_requirements
from dagster.core.errors import DagsterUnmetExecutorRequirementsError
from dagster.core.events import DagsterEvent, DagsterEventType, EngineEventData, MetadataEntry
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.execution.retries import RetryMode, get_retries_config
from dagster.core.executor.base import Executor
from dagster.core.executor.init import InitExecutorContext
from dagster.core.executor.step_delegating import StepDelegatingExecutor
from dagster.core.executor.step_delegating.step_handler import StepHandler
from dagster.core.executor.step_delegating.step_handler.base import StepHandlerContext
from dagster.core.types.dagster_type import Optional
from dagster.utils import frozentags, merge_dicts

from .container_context import K8sContainerContext
from .job import (
    DagsterK8sJobConfig,
    construct_dagster_k8s_job,
    get_k8s_job_name,
    get_user_defined_k8s_config,
)
from .utils import delete_job


@executor(
    name="k8s",
    config_schema=merge_dicts(
        DagsterK8sJobConfig.config_type_job(),
        {"job_namespace": Field(StringSource, is_required=False)},
        {"retries": get_retries_config()},
    ),
    requirements=multiple_process_executor_requirements(),
)
def k8s_job_executor(init_context: InitExecutorContext) -> Executor:
    """
    Executor which launches steps as Kubernetes Jobs.

    To use the `k8s_job_executor`, set it as the `executor_def` when defining a job:

    .. literalinclude:: ../../../../../../python_modules/libraries/dagster-k8s/dagster_k8s_tests/unit_tests/test_example_executor_mode_def.py
       :start-after: start_marker
       :end-before: end_marker
       :language: python

    Then you can configure the executor with run config as follows:

    .. code-block:: YAML

        execution:
          config:
            job_namespace: 'some-namespace'
            image_pull_policy: ...
            image_pull_secrets: ...
            service_account_name: ...
            env_config_maps: ...
            env_secrets: ...
            env_vars: ...
            job_image: ... # leave out if using userDeployments

    Configuration set on the Kubernetes Jobs and Pods created by the `K8sRunLauncher` will also be
    set on Kubernetes Jobs and Pods created by the `k8s_job_executor`.
    """

    run_launcher = init_context.instance.run_launcher
    if not isinstance(run_launcher, K8sRunLauncher):
        raise DagsterUnmetExecutorRequirementsError(
            "This engine is only compatible with a K8sRunLauncher; configure the "
            "K8sRunLauncher on your instance to use it.",
        )

    exc_cfg = init_context.executor_config

    k8s_container_context = K8sContainerContext(
        image_pull_policy=exc_cfg.get("image_pull_policy"),
        image_pull_secrets=exc_cfg.get("image_pull_secrets"),
        service_account_name=exc_cfg.get("service_account_name"),
        env_config_maps=exc_cfg.get("env_config_maps"),
        env_secrets=exc_cfg.get("env_secrets"),
        env_vars=exc_cfg.get("env_vars"),
        volume_mounts=exc_cfg.get("volume_mounts"),
        volumes=exc_cfg.get("volumes"),
        labels=exc_cfg.get("labels"),
        namespace=exc_cfg.get("job_namespace"),
    )

    return StepDelegatingExecutor(
        K8sStepHandler(
            image=exc_cfg.get("job_image"),
            container_context=k8s_container_context,
            load_incluster_config=run_launcher.load_incluster_config,
            kubeconfig_file=run_launcher.kubeconfig_file,
        ),
        retries=RetryMode.from_config(init_context.executor_config["retries"]),
        should_verify_step=True,
    )


class K8sStepHandler(StepHandler):
    @property
    def name(self):
        return "K8sStepHandler"

    def __init__(
        self,
        image: Optional[str],
        container_context: K8sContainerContext,
        load_incluster_config: bool,
        kubeconfig_file: Optional[str],
        k8s_client_batch_api=None,
    ):
        super().__init__()

        self._executor_image = check.opt_str_param(image, "image")
        self._executor_container_context = check.inst_param(
            container_context, "container_context", K8sContainerContext
        )

        self._fixed_k8s_client_batch_api = k8s_client_batch_api

        if load_incluster_config:
            check.invariant(
                kubeconfig_file is None,
                "`kubeconfig_file` is set but `load_incluster_config` is True.",
            )
            kubernetes.config.load_incluster_config()
        else:
            check.opt_str_param(kubeconfig_file, "kubeconfig_file")
            kubernetes.config.load_kube_config(kubeconfig_file)

    def _get_container_context(self, step_handler_context: StepHandlerContext):
        run_target = K8sContainerContext.create_for_run(
            step_handler_context.pipeline_run, step_handler_context.instance.run_launcher
        )
        return run_target.merge(self._executor_container_context)

    @property
    def _batch_api(self):
        return self._fixed_k8s_client_batch_api or kubernetes.client.BatchV1Api()

    def _get_k8s_step_job_name(self, step_handler_context):
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[0]

        name_key = get_k8s_job_name(
            step_handler_context.execute_step_args.pipeline_run_id,
            step_key,
        )

        if step_handler_context.execute_step_args.known_state:
            retry_state = step_handler_context.execute_step_args.known_state.get_retry_state()
            if retry_state.get_attempt_count(step_key):
                return "dagster-step-%s-%d" % (name_key, retry_state.get_attempt_count(step_key))

        return "dagster-step-%s" % (name_key)

    def launch_step(self, step_handler_context: StepHandlerContext):
        events = []

        assert (
            len(step_handler_context.execute_step_args.step_keys_to_execute) == 1
        ), "Launching multiple steps is not currently supported"
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[0]

        job_name = self._get_k8s_step_job_name(step_handler_context)
        pod_name = job_name

        args = step_handler_context.execute_step_args.get_command_args()

        container_context = self._get_container_context(step_handler_context)

        job_config = container_context.get_k8s_job_config(
            self._executor_image, step_handler_context.instance.run_launcher
        )

        if not job_config.job_image:
            job_config = job_config.with_image(
                step_handler_context.execute_step_args.pipeline_origin.repository_origin.container_image
            )

        if not job_config.job_image:
            raise Exception("No image included in either executor config or the job")

        user_defined_k8s_config = get_user_defined_k8s_config(
            frozentags(step_handler_context.step_tags[step_key])
        )

        job = construct_dagster_k8s_job(
            job_config=job_config,
            args=args,
            job_name=job_name,
            pod_name=pod_name,
            component="step_worker",
            user_defined_k8s_config=user_defined_k8s_config,
            labels={
                "dagster/job": step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                "dagster/op": step_key,
                "dagster/run-id": step_handler_context.execute_step_args.pipeline_run_id,
            },
        )

        events.append(
            DagsterEvent(
                event_type_value=DagsterEventType.ENGINE_EVENT.value,
                pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                step_key=step_key,
                message=f"Executing step {step_key} in Kubernetes job {job_name}",
                event_specific_data=EngineEventData(
                    [
                        MetadataEntry("Step key", value=step_key),
                        MetadataEntry("Kubernetes Job name", value=job_name),
                    ],
                ),
            )
        )

        self._batch_api.create_namespaced_job(body=job, namespace=container_context.namespace)

        return events

    def check_step_health(self, step_handler_context: StepHandlerContext):
        assert (
            len(step_handler_context.execute_step_args.step_keys_to_execute) == 1
        ), "Launching multiple steps is not currently supported"
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[0]

        job_name = self._get_k8s_step_job_name(step_handler_context)

        container_context = self._get_container_context(step_handler_context)

        job = self._batch_api.read_namespaced_job(
            namespace=container_context.namespace, name=job_name
        )
        if job.status.failed:
            return [
                DagsterEvent(
                    event_type_value=DagsterEventType.STEP_FAILURE.value,
                    pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                    step_key=step_key,
                    message=f"Discovered failed Kubernetes job {job_name} for step {step_key}",
                    event_specific_data=StepFailureData(
                        error=None,
                        user_failure_data=None,
                    ),
                )
            ]
        return []

    def terminate_step(self, step_handler_context: StepHandlerContext):
        assert (
            len(step_handler_context.execute_step_args.step_keys_to_execute) == 1
        ), "Launching multiple steps is not currently supported"

        job_name = self._get_k8s_step_job_name(step_handler_context)
        container_context = self._get_container_context(step_handler_context)

        delete_job(job_name=job_name, namespace=container_context.namespace)
        return []
