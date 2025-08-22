import os
import subprocess
import sys
import time

def run():
    """
    Detects the environment and launches the bot and its workers
    with a memory-efficient, high-concurrency configuration.
    """
    # --- Intelligent Concurrency Logic for Gevent ---
    # For Gevent, concurrency is not tied to CPU cores but to I/O.
    # A high number is standard and allows handling many network tasks efficiently.
    # This can be overridden for fine-tuning on the VPS.
    worker_concurrency = os.getenv("WORKER_CONCURRENCY", "100")
    accelerator_concurrency = os.getenv("ACCELERATOR_CONCURRENCY", "100")

    print(f"🚀 Launching with Gevent pool configuration:")
    print(f"   - Bot Listener: 1 process")
    print(f"   - Standard Worker Concurrency: {worker_concurrency}")
    print(f"   - Accelerator Worker Concurrency: {accelerator_concurrency}")
    print("-" * 30)

    # Define the commands to be run using the Gevent pool
    commands = {
        "bot": "python bot/bot.py",
        "worker": f"celery -A worker.tasks worker --loglevel=info -Q default --pool=gevent -c {worker_concurrency}",
        "accelerator": f"celery -A worker.tasks worker --loglevel=info -Q high_priority --pool=gevent -c {accelerator_concurrency}"
    }

    processes = {}
    try:
        for name, cmd in commands.items():
            proc = subprocess.Popen(cmd.split(), stdout=sys.stdout, stderr=sys.stderr)
            processes[name] = proc
            print(f"   -> Process '{name}' started with PID: {proc.pid}")
            time.sleep(2)

        print("\n✅ All processes have been launched.")
        print("   Use Ctrl+C to terminate the launcher and all child processes.")
        
        # Keep the launcher alive to monitor child processes
        while True:
            for name, proc in processes.items():
                if proc.poll() is not None:
                    print(f"\n🚨 WARNING: Process '{name}' has terminated unexpectedly. Restarting...")
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
