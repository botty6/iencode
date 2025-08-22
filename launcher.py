import os
import subprocess
import sys
import time

def run():
    """
    Launches the bot and its specialized workers with a robust, hybrid configuration.
    - Gevent for I/O-bound download tasks.
    - Prefork for CPU-bound encode tasks.
    """
    try:
        cpu_cores = os.cpu_count()
    except NotImplementedError:
        cpu_cores = 2
    
    print(f"✅ Detected {cpu_cores} CPU cores.")

    # --- Intelligent Concurrency Logic ---
    if cpu_cores <= 2: # Heroku-safe settings
        worker_concurrency = 1
        accelerator_concurrency = 1
    elif cpu_cores <= 4:
        worker_concurrency = 1
        accelerator_concurrency = cpu_cores - 1
    else:
        worker_concurrency = max(2, cpu_cores // 4)
        accelerator_concurrency = cpu_cores - worker_concurrency

    # Gevent concurrency is for I/O, can be high
    io_worker_concurrency = os.getenv("IO_WORKER_CONCURRENCY", "100")

    print("🚀 Launching with HYBRID pool configuration:")
    print(f"   - Bot Listener: 1 process")
    print(f"   - I/O Worker (Gevent): {io_worker_concurrency} concurrency")
    print(f"   - Standard CPU Worker (Prefork): {worker_concurrency} cores")
    print(f"   - Accelerator CPU Worker (Prefork): {accelerator_concurrency} cores")
    print("-" * 30)

    commands = {
        "bot": "python bot/bot.py",
        "io_worker": f"celery -A worker.tasks worker --loglevel=info -Q io_queue -P gevent -c {io_worker_concurrency}",
        "worker": f"celery -A worker.tasks worker --loglevel=info -Q default -P prefork -c {worker_concurrency}",
        "accelerator": f"celery -A worker.tasks worker --loglevel=info -Q high_priority -P prefork -c {accelerator_concurrency}"
    }

    processes = {}
    try:
        for name, cmd in commands.items():
            proc = subprocess.Popen(cmd.split(), stdout=sys.stdout, stderr=sys.stderr)
            processes[name] = proc
            print(f"   -> Process '{name}' started with PID: {proc.pid}")
            time.sleep(2)

        print("\n✅ All processes have been launched.")
        print("   Use Ctrl+C to terminate.")
        
        while True:
            for name, proc in processes.items():
                if proc.poll() is not None:
                    print(f"\n🚨 WARNING: Process '{name}' has terminated. Restarting...")
                    new_proc = subprocess.Popen(commands[name].split(), stdout=sys.stdout, stderr=sys.stderr)
                    processes[name] = new_proc
                    print(f"   -> Process '{name}' restarted with PID: {new_proc.pid}")
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down all processes...")
        for proc in processes.values():
            proc.terminate()
        for proc in processes.values():
            proc.wait()
        print("✅ Shutdown complete.")

if __name__ == "__main__":
    run()
