import ray
import logging
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi import (
    Form,
    status,
    FastAPI,
    Depends,
    Request,
    Response,
    Security,
    WebSocket,
    HTTPException,
    WebSocketException,
    BackgroundTasks,
)
from lib.state import lifespan, BackendState
from lib.job.base import Job, JobResult
from lib.types import *

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
    return JSONResponse({'job_types': list(state.job_types.keys())})

@app.get('/job/list', tags=['job'], operation_id='list_jobs')
async def get_job_list(
    state: BackendState = Depends(get_state_from_request)
) -> JobResultResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

@app.get('/job', tags=['job'], operation_id='get_job_info')
async def get_job_info(
    job_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> JobResultResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

@app.post('/job', tags=['job'], operation_id='submit_job')
async def submit_job(
    request: JobSubmitRequest,
    state: BackendState = Depends(get_state_from_request),
) -> JobResultResponse:
    if not request.job_type in state.job_types:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail='Unknown job type')
    
    job_info = state.job_types[request.job_type]
    logger.info(f'Launching job type {request.job_type} of {job_info["job_cls"]}')

    job_kwargs = {k: v for k,v in (request.__pydantic_extra__ or {}).items()}
    logger.info(f'Job kwargs: {job_kwargs}')
    
    job = job_info['job_cls'](**job_kwargs)
    logger.info(f'Job ID is {job.job_id}')

    as_background = request.as_background and job.supports_background
    async with state.connection:
        input = job.setup()
        if not as_background:
            # synchronous run, hang up until task is completed
            # TODO: add timeout
            result = job.run(**(input or {}))
            logger.info(f'Job {job.job_id} has successfully finished')
        else:
            # asynchronous run, doesn't care about task results
            # TODO: add some result retrieval mechanizm
            futures = job.start(**(input or {}))
            result = None
            logger.info(f'Job {job.job_id} has started as background task')

        match result:
            case JobResult():
                return JobResultResponse(
                    job_results=[result], 
                    as_background=as_background)
            case tuple() | list() if not len(result):
                return JobResultResponse(
                    job_results=[], 
                    as_background=as_background)
            case tuple() | list() if isinstance(result[0], JobResult):
                return JobResultResponse(
                    job_results=result,     #type:ignore
                    as_background=as_background)
            case _:
                return JobResultResponse(
                    job_results=[JobResult.from_job(job, output=result)], 
                    as_background=as_background)

@app.post('/job/pause', tags=['job'], operation_id='pause_job')
async def pause_job(
    job_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> JobResultResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

@app.post('/job/resume', tags=['job'], operation_id='resume_job')
async def resume_job(
    job_id: str,
    state: BackendState = Depends(get_state_from_request)
) -> JobResultResponse:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail='Not implemented')

@app.websocket('/job')
async def job_stream(
    websocket: WebSocket,
    state: BackendState = Depends(get_state_from_websocket),
):
    await websocket.accept()
    try:
        while True:
            msg = await websocket.receive_json()
            await websocket.send_json(msg)

    except Exception as ex:
        # unrecoverable error, close the websocket
        raise WebSocketException(status.WS_1011_INTERNAL_ERROR, str(ex))
    finally:
        await websocket.close()

#
# Static files
#
app.mount('/static', StaticFiles(directory='www/static', html=True), name='static')
app.mount('/data', StaticFiles(directory='www/data', html=True), name='data')
