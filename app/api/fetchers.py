from fastapi import APIRouter, HTTPException, status
from app.services.github_fetcher import github_fetcher
from app.services.hn_fetcher import hn_fetcher

router = APIRouter(prefix="/fetchers/github", tags=["GitHub Fetcher"])

@router.post("/once")
async def fetch_github_once():
    try:
        count = await github_fetcher.fetch_and_push()
        return {
            "status": "success",
            "message": f"Успешно заброшено {count} событий из GitHub в очередь Redis",
            "pushed_events": count}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при обращении к GitHub API: {str(e)}")

@router.post("/start")
async def start_github_polling(interval: int = 10):
    started = github_fetcher.start_background_polling(interval_seconds=interval)
    if not started:
        return {"status": "warning", "message": "Фоновый опрос GitHub уже запущен и работает!"}
    return {
        "status": "success",
        "message": f"Автоматическая откачка GitHub запущен (интервал: {interval} сек)"}

@router.post("/stop")
async def stop_github_polling():
    stopped = await github_fetcher.stop_background_polling()
    if not stopped:
        return {"status": "warning", "message": "Фоновый опрос не был запущен."}
    return {"status": "success", "message": "Автоматический опрос GitHub успешно остановлен"}

@router.get("/status")
async def get_github_fetcher_status():
    return {
        "is_running": github_fetcher.is_running}

@router.post("/hn/once")
async def fetch_hn_once(limit: int = 15):
    try:
        count = await hn_fetcher.fetch_and_push(limit=limit)
        return {
            "status": "success",
            "message": f"Успешно заброшено {count} новостей из Hacker News в очередь Redis",
            "pushed_events": count,}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при обращении к Hacker News API: {str(e)}",)

@router.post("/hn/start")
async def start_hn_polling(interval: int = 15):
    started = hn_fetcher.start_background_polling(interval_seconds=interval)
    if not started:
        return {"status": "warning", "message": "Фоновый опрос Hacker News уже запущен!"}
    return {"status": "success", "message": f"Автоматический опрос Hacker News запущен (интервал: {interval} сек)"}

@router.post("/hn/stop")
async def stop_hn_polling():
    stopped = await hn_fetcher.stop_background_polling()
    if not stopped:
        return {"status": "warning", "message": "Фоновый опрос Hacker News не был запущен."}
    return {"status": "success", "message": "Автоматический опрос Hacker News успешно остановлен"}

@router.get("/hn/status")
async def get_hn_fetcher_status():
    return {"is_running": hn_fetcher.is_running}