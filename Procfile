bot: python bot/bot.py
accelerator: celery -A worker.tasks worker --loglevel=info -Q high_priority --concurrency=2
worker: celery -A worker.tasks worker --loglevel=info -Q default --concurrency=1
