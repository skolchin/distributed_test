import ray
import json
import logging
from numpy.typing import NDArray
from ray.exceptions import RayTaskError
from datetime import datetime
from sqlmodel import Session, select
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi import (
    status,
    FastAPI,
    Depends,
    Request,
    WebSocket,
    HTTPException,
    BackgroundTasks,
)

from lib.responses import *
from lib.state import lifespan, BackendState
from lib.json_utils import CustomJsonEncoder
from lib.job import Job, Task, CannotSerializeResult

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
# Compute jobs
#
@app.get('/job/types', tags=['compute'], operation_id='list_job_types')
async def get_job_types_list(
    state: BackendState = Depends(get_state_from_request)
) -> JobTypesResponse:
    """ Retrieve available job types """
    return JobTypesResponse(job_types=list(state.job_types.keys()))

@app.get('/job/list', tags=['compute'], operation_id='list_jobs')
async def get_job_list(
    state: BackendState = Depends(get_state_from_request)
) -> JobListResponse:
    """ Retrieve list of jobs """
    with Session(state.engine) as session:
        jobs = session.exec(select(Job)).all()
        return JobListResponse.from_jobs(jobs)

@app.get('/job', tags=['compute'], operation_id='get_job_info')
async def get_job_info(
    job_id: str,
    state: BackendState = Depends(get_state_from_request),
) -> JobResponse:
    """ Retrieve single job info """
    with Session(state.engine) as session:
        try:
            job = session.exec(select(Job).where(Job.job_id == job_id)).one()
            return JobResponse.from_job(job)
        except:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f'Job {job_id} not found')

@app.get('/job/result', tags=['compute'], operation_id='get_job_result')
async def get_job_result(
    task_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> Response:
    """ Retrieve job results """
    with Session(state.engine) as session:
        try:
            task = session.exec(select(Task).where(Task.task_id == task_id)).one()
        except:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f'Task {task_id} not found')

        if (output := task.get_raw_output(state.options)) is None:
            raise HTTPException(status.HTTP_410_GONE, f'Task {task_id} results are expired')
        
        return Response(
            content=json.dumps(output, ensure_ascii=False, cls=CustomJsonEncoder),
            media_type='application/json')

        # import io
        # stream = io.BytesIO()
        # for j in job.tasks:
        #     stream.write(j.full_output or bytes())
        # stream.seek(0)
        # buf = stream.getvalue()

        # if not chunked:
        #     return Response(
        #         content=buf,
        #         status_code=status.HTTP_206_PARTIAL_CONTENT,
        #         media_type='application/octet-stream',
        #     )

        # page_size = state.options.max_inline_result_size
        # max_chunk = int(len(buf) / page_size + 1)
        # if chunk_no < 0:
        #     return Response(
        #         content='{ "max_chunk": ' + str(max_chunk) + ', "page_size": ' + str(page_size) + '}',
        #         media_type='application/json'
        #     )
        # if chunk_no >= max_chunk:
        #     return Response(
        #         content='No more data',
        #         status_code=status.HTTP_404_NOT_FOUND,
        #         media_type='text/plain',
        #     )

        # pos = chunk_no * page_size
        # return Response(
        #     content=buf[pos:pos+page_size],
        #     status_code=status.HTTP_206_PARTIAL_CONTENT,
        #     media_type='application/octet-stream',
        # )

@app.post('/job', tags=['compute'], operation_id='submit_job')
async def submit_job(
    request: JobSubmitRequest,
    background_tasks: BackgroundTasks,
    state: BackendState = Depends(get_state_from_request),
) -> JobResponse:
    """ Submit new job """

    if not request.job_type in state.job_types:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f'Unknown job type {request.job_type}')

    job_info = state.job_types[request.job_type]
    logger.info(f'Launching job of {request.job_type} type')

    job_kwargs = {k: v for k,v in (request.__pydantic_extra__ or {}).items()}
    logger.info(f'Job kwargs: {job_kwargs}')

    if job_info['job_cls']:
        job = job_info['job_cls'](
            job_type=job_info['job_cls'].default_job_type,
            job_kwargs=job_kwargs,
        )
    elif job_info['job_func']:
        job: Job = job_info['job_func']()
    else:
        raise ValueError(f'Neither class nor function')
    
    logger.info(f'Job ID is {job.job_id}')

    async def save_job(job: Job):
        with Session(state.engine, expire_on_commit=False) as session:
            session.add(job)
            session.commit()
            session.expunge(job)

    async def run_job(job: Job, is_background: bool = False, stream: bool = False) -> List[Task]:
        with Session(state.engine, expire_on_commit=False) as session:
            session.add(job)
            session.commit()

            async with state.connection:
                if not stream:
                    logger.info(f'Job {job.job_id} is to be started in sync mode')
                    tasks = job.run(options=state.options, is_background=is_background, **job_kwargs)
                    logger.info(f'Job {job.job_id} has successfully finished')
                else:
                    logger.info(f'Job {job.job_id} is to be started in streaming mode')
                    tasks = [t async for t in job.stream(options=state.options, is_background=is_background, **job_kwargs)]
                    logger.info(f'Job {job.job_id} has successfully finished')

            dt_now = datetime.now()
            for task in tasks:
                task.finished = dt_now
                session.merge(task)

            session.commit()
            session.refresh(job)
            return tasks


    as_background = request.as_background and job.supports_background
    if not as_background:
        # synchronous run, hang up until task is completed
        await run_job(job, is_background=False, stream=request.stream)
    else:
        # save job
        await save_job(job)

        # asynchronous run
        background_tasks.add_task(
            run_job,
            job=job,
            is_background=True,
            stream=request.stream)
        logger.info(f'Job {job.job_id} has started as a background task')

    # return await get_job_info(job.job_id, state)
    # return job
    return JobResponse.from_job(job, add_tasks=not as_background)

@app.post('/job/cancel', tags=['compute'], operation_id='cancel_job')
async def cancel_job(
    job_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> JobResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

@app.websocket('/job')
async def job_stream(
    websocket: WebSocket,
    state: BackendState = Depends(get_state_from_websocket),
):
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

#
# Exception handling
#
@app.exception_handler(CannotSerializeResult)
def handle_serialization_error(request: Request, ex: CannotSerializeResult):
    logger.error(str(ex))
    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex))

@app.exception_handler(RayTaskError)
def handle_ray_error(request: Request, ex: RayTaskError):
    logger.error(str(ex))
    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex.cause or ex))

#
# Static files
#
app.mount('/static', StaticFiles(directory='www/static', html=True), name='static')
