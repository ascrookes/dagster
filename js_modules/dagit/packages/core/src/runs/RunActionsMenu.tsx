import {gql, useLazyQuery, useMutation} from '@apollo/client';
import {
  Button,
  HighlightedCodeBlock,
  Icon,
  MenuDivider,
  MenuExternalLink,
  MenuItem,
  Menu,
  Popover,
  Tooltip,
} from '@dagster-io/ui';
import * as React from 'react';
import {useHistory} from 'react-router-dom';
import * as yaml from 'yaml';

import {AppContext} from '../app/AppContext';
import {showCustomAlert} from '../app/CustomAlertProvider';
import {usePermissions} from '../app/Permissions';
import {MenuLink} from '../ui/MenuLink';
import {isThisThingAJob} from '../workspace/WorkspaceContext';
import {useRepositoryForRun} from '../workspace/useRepositoryForRun';
import {workspacePathFromRunDetails} from '../workspace/workspacePath';

import {DeletionDialog} from './DeletionDialog';
import {RUN_FRAGMENT_FOR_REPOSITORY_MATCH} from './RunFragments';
import {doneStatuses} from './RunStatuses';
import {
  LAUNCH_PIPELINE_REEXECUTION_MUTATION,
  RunsQueryRefetchContext,
  getReexecutionVariables,
  handleLaunchResult,
} from './RunUtils';
import {TerminationDialog} from './TerminationDialog';
import {LaunchPipelineReexecution} from './types/LaunchPipelineReexecution';
import {PipelineEnvironmentYamlQuery} from './types/PipelineEnvironmentYamlQuery';
import {RunTableRunFragment} from './types/RunTableRunFragment';

export const RunActionsMenu: React.FC<{
  run: RunTableRunFragment;
}> = React.memo(({run}) => {
  const {refetch} = React.useContext(RunsQueryRefetchContext);
  const [visibleDialog, setVisibleDialog] = React.useState<'none' | 'terminate' | 'delete'>('none');

  const {rootServerURI} = React.useContext(AppContext);
  const {canTerminatePipelineExecution, canDeletePipelineRun} = usePermissions();
  const history = useHistory();

  const [reexecute] = useMutation<LaunchPipelineReexecution>(LAUNCH_PIPELINE_REEXECUTION_MUTATION, {
    onCompleted: refetch,
  });

  const [loadEnv, {called, loading, data}] = useLazyQuery<PipelineEnvironmentYamlQuery>(
    PIPELINE_ENVIRONMENT_YAML_QUERY,
    {
      variables: {runId: run.runId},
    },
  );

  const closeDialogs = () => {
    setVisibleDialog('none');
  };

  const onComplete = () => {
    refetch();
  };

  const pipelineRun =
    data?.pipelineRunOrError?.__typename === 'Run' ? data?.pipelineRunOrError : null;
  const runConfigYaml = pipelineRun?.runConfigYaml;

  const repoMatch = useRepositoryForRun(pipelineRun);
  const isFinished = doneStatuses.has(run.status);
  const isJob = !!(repoMatch && isThisThingAJob(repoMatch?.match, run.pipelineName));

  const infoReady = called ? !loading : false;
  return (
    <>
      <Popover
        content={
          <Menu>
            <MenuItem
              text={loading ? 'Loading Configuration...' : 'View Configuration...'}
              disabled={!runConfigYaml}
              icon="open_in_new"
              onClick={() =>
                showCustomAlert({
                  title: 'Config',
                  body: <HighlightedCodeBlock value={runConfigYaml || ''} language="yaml" />,
                })
              }
            />
            <MenuDivider />
            <>
              <Tooltip
                content={OPEN_LAUNCHPAD_UNKNOWN}
                position="bottom"
                disabled={infoReady}
                targetTagName="div"
              >
                <MenuLink
                  text="Open in Launchpad..."
                  disabled={!infoReady}
                  icon="edit"
                  to={workspacePathFromRunDetails({
                    id: run.id,
                    pipelineName: run.pipelineName,
                    repositoryName: repoMatch?.match.repository.name,
                    repositoryLocationName: repoMatch?.match.repositoryLocation.name,
                    isJob,
                  })}
                />
              </Tooltip>
              <Tooltip
                content="Re-execute is unavailable because the pipeline is not present in the current workspace."
                position="bottom"
                disabled={infoReady && !!repoMatch}
                targetTagName="div"
              >
                <MenuItem
                  text="Re-execute"
                  disabled={!infoReady || !repoMatch}
                  icon="refresh"
                  onClick={async () => {
                    if (repoMatch && runConfigYaml) {
                      const runConfig = yaml.parse(runConfigYaml);
                      const result = await reexecute({
                        variables: getReexecutionVariables({
                          run: {...run, runConfig},
                          style: {type: 'all'},
                          repositoryLocationName: repoMatch.match.repositoryLocation.name,
                          repositoryName: repoMatch.match.repository.name,
                        }),
                      });
                      handleLaunchResult(run.pipelineName, result, history, {
                        behavior: 'open',
                      });
                    }
                  }}
                />
              </Tooltip>
              {isFinished || !canTerminatePipelineExecution ? null : (
                <MenuItem
                  icon="cancel"
                  text="Terminate"
                  onClick={() => setVisibleDialog('terminate')}
                />
              )}
              <MenuDivider />
            </>
            <MenuExternalLink
              text="Download Debug File"
              icon="download_for_offline"
              download
              href={`${rootServerURI}/download_debug/${run.runId}`}
            />
            {canDeletePipelineRun ? (
              <MenuItem
                icon="delete"
                text="Delete"
                intent="danger"
                onClick={() => setVisibleDialog('delete')}
              />
            ) : null}
          </Menu>
        }
        position="bottom-right"
        onOpening={() => {
          if (!called) {
            loadEnv();
          }
        }}
      >
        <Button icon={<Icon name="expand_more" />} />
      </Popover>
      {canTerminatePipelineExecution ? (
        <TerminationDialog
          isOpen={visibleDialog === 'terminate'}
          onClose={closeDialogs}
          onComplete={onComplete}
          selectedRuns={{[run.id]: run.canTerminate}}
        />
      ) : null}
      {canDeletePipelineRun ? (
        <DeletionDialog
          isOpen={visibleDialog === 'delete'}
          onClose={closeDialogs}
          onComplete={onComplete}
          onTerminateInstead={() => setVisibleDialog('terminate')}
          selectedRuns={{[run.id]: run.canTerminate}}
        />
      ) : null}
    </>
  );
});

export const RunBulkActionsMenu: React.FC<{
  selected: RunTableRunFragment[];
  clearSelection: () => void;
}> = React.memo(({selected, clearSelection}) => {
  const {refetch} = React.useContext(RunsQueryRefetchContext);
  const {canTerminatePipelineExecution, canDeletePipelineRun} = usePermissions();
  const [visibleDialog, setVisibleDialog] = React.useState<'none' | 'terminate' | 'delete'>('none');

  if (!canTerminatePipelineExecution && !canDeletePipelineRun) {
    return null;
  }

  const unfinishedRuns = selected.filter((r) => !doneStatuses.has(r?.status));
  const unfinishedIDs = unfinishedRuns.map((r) => r.id);
  const unfinishedMap = unfinishedRuns.reduce(
    (accum, run) => ({...accum, [run.id]: run.canTerminate}),
    {},
  );

  const selectedIDs = selected.map((run) => run.runId);
  const deletionMap = selected.reduce((accum, run) => ({...accum, [run.id]: run.canTerminate}), {});

  const closeDialogs = () => {
    setVisibleDialog('none');
  };

  const onComplete = () => {
    clearSelection();
    refetch();
  };

  return (
    <>
      <Popover
        content={
          <Menu>
            {canTerminatePipelineExecution ? (
              <MenuItem
                icon="cancel"
                text={`Terminate ${unfinishedIDs.length} ${
                  unfinishedIDs.length === 1 ? 'run' : 'runs'
                }`}
                disabled={unfinishedIDs.length === 0}
                onClick={() => {
                  setVisibleDialog('terminate');
                }}
              />
            ) : null}
            {canDeletePipelineRun ? (
              <MenuItem
                icon="delete"
                intent="danger"
                text={`Delete ${selectedIDs.length} ${selectedIDs.length === 1 ? 'run' : 'runs'}`}
                disabled={selectedIDs.length === 0}
                onClick={() => {
                  setVisibleDialog('delete');
                }}
              />
            ) : null}
          </Menu>
        }
        position="bottom-right"
      >
        <Button disabled={selected.length === 0} rightIcon={<Icon name="expand_more" />}>
          Actions
        </Button>
      </Popover>
      <TerminationDialog
        isOpen={visibleDialog === 'terminate'}
        onClose={closeDialogs}
        onComplete={onComplete}
        selectedRuns={unfinishedMap}
      />
      <DeletionDialog
        isOpen={visibleDialog === 'delete'}
        onClose={closeDialogs}
        onComplete={onComplete}
        onTerminateInstead={() => setVisibleDialog('terminate')}
        selectedRuns={deletionMap}
      />
    </>
  );
});

const OPEN_LAUNCHPAD_UNKNOWN =
  'Launchpad is unavailable because the pipeline is not present in the current repository.';

// Avoid fetching envYaml on load in Runs page. It is slow.
const PIPELINE_ENVIRONMENT_YAML_QUERY = gql`
  query PipelineEnvironmentYamlQuery($runId: ID!) {
    pipelineRunOrError(runId: $runId) {
      ... on Run {
        id
        pipelineName
        pipelineSnapshotId
        runConfigYaml
        ...RunFragmentForRepositoryMatch
      }
    }
  }
  ${RUN_FRAGMENT_FOR_REPOSITORY_MATCH}
`;
