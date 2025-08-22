bot: python bot/bot.py
accelerator: celery -A worker.tasks worker --loglevel=info -Q high_priority --concurrency=8
worker: celery -A worker.tasks worker --loglevel=info -Q default --concurrency=2
