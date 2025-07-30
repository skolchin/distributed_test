import ray
import logging
from datetime import datetime
from sqlmodel import Session, select
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi import (
    status,
    FastAPI,
    Depends,
    Request,
    WebSocket,
    HTTPException,
    BackgroundTasks,
)
from lib.state import lifespan, BackendState
from lib.job.job import Job
from lib.job.job_instance import JobInstance
from lib.types import *
from typing import cast

logging.basicConfig(
    format='[%(levelname).1s %(asctime)s %(name)s] %(message)s', 
    level=logging.DEBUG, 
    force=True)

logger = logging.getLogger(__name__)

app = FastAPI(
    title='Ray service',
    description='Ray backend service',
    lifespan=lifespan,
)

def get_state_from_request(request: Request) -> BackendState:
    """ Get app state from request """
    return request.app.state.backend_state

def get_state_from_websocket(websocket: WebSocket) -> BackendState:
    """ Get app state from websocket """
    return websocket.app.state.backend_state

@app.get("/", include_in_schema=False)
async def get_index(
    request: Request,
    state: BackendState = Depends(get_state_from_request)
) -> HTMLResponse:
    """ Generate index.html from template """
    return state.templates.TemplateResponse(
        request=request, 
        name='index.html', 
        context={
        }
    )

#
# Cluster
#
@app.get('/cluster', tags=['cluster'], operation_id='cluster_status')
async def get_cluster_status(
    state: BackendState = Depends(get_state_from_request)
) -> ClusterStatusResponse:
    """ Cluster status """
    async with state.connection:
        return ClusterStatusResponse(
            cluster_address=state.options.cluster_address,
            nodes=ray.nodes(),
            resources=ray.cluster_resources(),
        )

# @app.post('/cluster/start', tags=['cluster'], operation_id='cluster_start')
# async def cluster_start(
#     state: BackendState = Depends(get_state_from_request)
# ) -> ClusterOperationResponse:
#     """ Start a cluster """
#     raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

# @app.post('/cluster/stop', tags=['cluster'], operation_id='cluster_stop')
# async def cluster_stop(
#     state: BackendState = Depends(get_state_from_request)
# ) -> ClusterOperationResponse:
#     """ Stop a cluster """
#     raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

#
# Node
#
@app.get('/node', tags=['node'], operation_id='node_status')
async def get_node_status(
    state: BackendState = Depends(get_state_from_request)
) -> NodeStatusResponse:
    """ Current node status """
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

# @app.post('/node/start', tags=['node'], operation_id='node_start')
# async def node_start(
#     state: BackendState = Depends(get_state_from_request)
# ) -> NodeOperationResponse:
#     raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

# @app.post('/node/stop', tags=['node'], operation_id='node_stop')
# async def node_stop(
#     state: BackendState = Depends(get_state_from_request)
# ) -> NodeOperationResponse:
#     raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

#
# Jobs
#
@app.get('/job/types', tags=['job'], operation_id='list_job_types')
async def get_job_types_list(
    state: BackendState = Depends(get_state_from_request)
) -> JSONResponse:
    """ Retrieve available job types """
    return JSONResponse({'job_types': list(state.job_types.keys())})

@app.get('/job/list', tags=['job'], operation_id='list_jobs')
async def get_job_list(
    state: BackendState = Depends(get_state_from_request)
) -> JobInstanceResponse:
    """ Retrieve list of jobs """
    with Session(state.engine) as session:
        job_instances = session.exec(select(JobInstance)).all()
        if not job_instances:
            jobs = {}
        else:
            jobs = {}
            for j in job_instances:
                if not j.job_id in jobs:
                    job_type = state.job_types[j.job_type]
                    jobs[j.job_id] = job_type['job_cls'](
                        job_id=job_instances[0].job_id,
                    )
        return JobInstanceResponse(
            jobs=list(jobs.values()),
            job_instances=job_instances,
        )

@app.get('/job', tags=['job'], operation_id='get_job_info')
async def get_job_info(
    job_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> JobInstanceResponse:
    """ Retrieve single job info """
    with Session(state.engine) as session:
        job_instances=session.exec(select(JobInstance).where(JobInstance.job_id == job_id)).all()
        if not job_instances:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f'Job {job_id} not found')

        job_type = state.job_types[job_instances[0].job_type]
        job = job_type['job_cls'](
            job_id=job_instances[0].job_id,
        )
        return JobInstanceResponse(
            jobs=[job],
            job_instances=job_instances,
        )

@app.post('/job', tags=['job'], operation_id='submit_job')
async def submit_job(
    request: JobSubmitRequest,
    background_tasks: BackgroundTasks,
    state: BackendState = Depends(get_state_from_request),
) -> JobInstanceResponse:
    """ Submit new job """
    
    if not request.job_type in state.job_types:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f'Unknown job type {request.job_type}')
    
    job_info = state.job_types[request.job_type]
    logger.info(f'Launching job type {request.job_type} of {job_info["job_cls"]}')

    job_kwargs = {k: v for k,v in (request.__pydantic_extra__ or {}).items()}
    logger.info(f'Job kwargs: {job_kwargs}')
    
    job = job_info['job_cls'](**job_kwargs)
    logger.info(f'Job ID is {job.job_id}')

    async def run_job(job: Job):
        async with state.connection:
            input = job.setup()
            task_result = job.run(**(input or {}))
            logger.info(f'Job {job.job_id} has successfully finished')
        return task_result

    def task_result_to_job_results(
            job: Job, 
            task_result, 
            placeholders: List[JobInstance] | None = None,
            is_background: bool = False,
            mark_finished: bool = True) -> List[JobInstance]:
        
        match task_result:
            case JobInstance():
                job_results=[task_result]
            case tuple() | list() if not len(task_result):
                job_results=[]
            case tuple() | list() if isinstance(task_result[0], JobInstance):
                job_results=task_result
            case _:
                job_results=[JobInstance.from_job(job, output=task_result)]

        dt_now = datetime.now()
        with Session(state.engine) as session:
            for job_result in job_results:
                job_result.is_background = is_background
                if mark_finished:
                    job_result.finished = dt_now
                session.add(job_result)
            for job_result in placeholders or []:
                session.delete(job_result)
            session.commit()
            for job_result in job_results:
                session.refresh(job_result)

        return cast(List[JobInstance], job_results)

    async def run_job_bg(job: Job, is_background: bool = False, placeholders: List[JobInstance] | None = None):
        task_result = await run_job(job)
        job_results = task_result_to_job_results(
            job, 
            task_result, 
            placeholders=placeholders, 
            is_background=is_background,
            mark_finished=True)
        return job_results

    as_background = request.as_background and job.supports_background
    if not as_background:
        # synchronous run, hang up until task is completed
        # TODO: add timeout
        task_result = await run_job(job)
        job_results = task_result_to_job_results(job, task_result, is_background=False, mark_finished=True)
    else:
        # save task placeholder
        job_results = task_result_to_job_results(job, None, is_background=True, mark_finished=False)

        # asynchronous run
        background_tasks.add_task(run_job_bg, job=job, is_background=True, placeholders=job_results)
        logger.info(f'Job {job.job_id} has started as a background task')

    return JobInstanceResponse(
        jobs=[job],
        job_instances=job_results
    )

@app.post('/job/cancel', tags=['job'], operation_id='cancel_job')
async def cancel_job(
    job_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> JobInstanceResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

@app.websocket('/job')
async def job_stream(
    websocket: WebSocket,
    state: BackendState = Depends(get_state_from_websocket),
):
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

#
# Static files
#
app.mount('/static', StaticFiles(directory='www/static', html=True), name='static')
app.mount('/data', StaticFiles(directory='www/data', html=True), name='data')
