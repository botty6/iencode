import os
import subprocess
import sys
import time

def run():
    """
    Detects system resources and launches the bot and its workers
    with an intelligent concurrency configuration.
    """
    try:
        cpu_cores = os.cpu_count()
        print(f"✅ Detected {cpu_cores} CPU cores.")
    except NotImplementedError:
        cpu_cores = 4 # Fallback for environments where detection fails
        print(f"⚠️ Could not detect CPU cores. Falling back to default: {cpu_cores}.")

    # --- Intelligent Concurrency Logic ---
    if cpu_cores <= 2:
        # For very small systems (like Heroku Basic)
        worker_concurrency = 1
        accelerator_concurrency = 1
    elif cpu_cores <= 4:
        # For small VPS
        worker_concurrency = 1
        accelerator_concurrency = cpu_cores - 1
    else:
        # For powerful VPS (8+ cores)
        # Reserve ~25% of cores for standard work, the rest for acceleration
        worker_concurrency = max(2, cpu_cores // 4)
        accelerator_concurrency = cpu_cores - worker_concurrency

    print(f"🚀 Launching with the following configuration:")
    print(f"   - Bot Listener: 1 process")
    print(f"   - Standard Worker: {worker_concurrency} parallel processes")
    print(f"   - Accelerator Worker: {accelerator_concurrency} parallel processes")
    print("-" * 30)

    # Define the commands to be run
    commands = {
        "bot": "python bot/bot.py",
        "worker": f"celery -A worker.tasks worker --loglevel=info -Q default --concurrency={worker_concurrency}",
        "accelerator": f"celery -A worker.tasks worker --loglevel=info -Q high_priority --concurrency={accelerator_concurrency}"
    }

    processes = {}
    for name, cmd in commands.items():
        # Using Popen to run commands in the background
        proc = subprocess.Popen(cmd.split(), stdout=sys.stdout, stderr=sys.stderr)
        processes[name] = proc
        print(f"   -> Process '{name}' started with PID: {proc.pid}")
        time.sleep(2) # Stagger startups

    print("\n✅ All processes have been launched.")
    print("   Use Ctrl+C to terminate the launcher and all child processes.")
    
    try:
        # Keep the launcher alive to monitor child processes
        while True:
            for name, proc in processes.items():
                if proc.poll() is not None:
                    print(f"\n🚨 WARNING: Process '{name}' has terminated unexpectedly. Restarting...")
                    new_proc = subprocess.Popen(commands[name].split(), stdout=sys.stdout, stderr=sys.stderr)
                    processes[name] = new_proc
                    print(f"   -> Process '{name}' restarted with PID: {new_proc.pid}")
            time.sleep(10) # Check every 10 seconds
    except KeyboardInterrupt:
        print("\n🛑 Shutting down all processes...")
        for proc in processes.values():
            proc.terminate()
        # Wait for all processes to terminate
        for proc in processes.values():
            proc.wait()
        print("✅ Shutdown complete.")

if __name__ == "__main__":
    run()
