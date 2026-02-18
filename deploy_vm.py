import paramiko
import time
import sys
import os

# Configuration
HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."
REPO_URL = "https://github.com/JoseLuis088/duma_planta.git"
APP_NAME = "duma_planta"
PORT = 8000

def run_command(ssh, command, sudo=False):
    if sudo:
        # Use -S to read password from stdin
        # We wrap the command in bash -c to support complex commands (&&, |, etc) even with sudo
        # But we must be careful with quotes.
        # Simplest way for common commands:
        cmd_str = command.replace("'", "'\\''") # Escape single quotes
        final_command = f"echo '{PASS}' | sudo -S -p '' bash -c '{cmd_str}'"
    else:
        final_command = command
    
    print(f"Running: {final_command}")
    stdin, stdout, stderr = ssh.exec_command(final_command)
    
    exit_status = stdout.channel.recv_exit_status()
    
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    
    if out: print(f"STDOUT: {out}")
    if err: print(f"STDERR: {err}")
    
    if exit_status != 0:
        print(f"Error executing command: {command}")
        return False
    return True

def main():
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(f"Connecting to {HOST}...")
        ssh.connect(HOST, username=USER, password=PASS)
        print("Connected.")

        # 1. Check/Install Docker
        print("\n--- Checking Docker ---")
        # Check without sudo first
        stdin, stdout, stderr = ssh.exec_command("docker --version")
        if stdout.channel.recv_exit_status() != 0:
            print("Docker not found. Installing...")
            commands = [
                "apt-get update",
                "apt-get install -y docker.io",
                "systemctl start docker",
                "systemctl enable docker",
                f"usermod -aG docker {USER}"
            ]
            for cmd in commands:
                if not run_command(ssh, cmd, sudo=True):
                    print("Failed to install Docker.")
                    return
        else:
             print("Docker is already installed.")

        # 2. Clone/Update Repo
        print("\n--- Updating Code ---")
        # Check if dir exists
        stdin, stdout, stderr = ssh.exec_command(f"ls -d {APP_NAME}")
        if stdout.channel.recv_exit_status() != 0:
            print("Cloning repo...")
            run_command(ssh, f"git clone {REPO_URL}")
        else:
            print("Pulling latest changes...")
            # We need to ensure we are not in a detached head state or dirty state
            # Force reset is safest for deployment
            run_command(ssh, f"cd {APP_NAME} && git fetch origin && git reset --hard origin/main")

        # 3. Upload .env
        print("\n--- Uploading .env ---")
        if os.path.exists(".env"):
            sftp = ssh.open_sftp()
            local_env = ".env"
            remote_env = f"/home/{USER}/{APP_NAME}/.env"
            print(f"Uploading .env to {remote_env}...")
            sftp.put(local_env, remote_env)
            
            # Upload ignored but critical AI files
            critical_files = ["main.py", "requirements.txt", "System prompt.txt", "duma_cookbook.txt", "schema.md"]
            for f in critical_files:
                if os.path.exists(f):
                    remote_f = f"/home/{USER}/{APP_NAME}/{f}"
                    print(f"Uploading {f} to {remote_f}...")
                    sftp.put(f, remote_f)
                
            sftp.close()
        else:
            print("WARNING: Local .env file not found!")

        # 4. Build Image
        print("\n--- Building Docker Image ---")
        # Use full path for build context to avoid cd issues if any, 
        # but with our new run_command wrapping in bash -c, 'cd && docker build' should work.
        # Let's try the safer path approach too.
        # Note: 'docker build -t name path' implies the Dockerfile is in that path.
        if not run_command(ssh, f"docker build -t {APP_NAME} /home/{USER}/{APP_NAME}", sudo=True):
            print("Build failed.")
            return

        # 5. Stop/Remove Old Container
        print("\n--- Restarting Container ---")
        run_command(ssh, f"docker stop {APP_NAME} || true", sudo=True)
        run_command(ssh, f"docker rm {APP_NAME} || true", sudo=True)

        # 6. Run New Container
        print("\n--- Running New Container ---")
        cmd_run = (
            f"docker run -d "
            f"--name {APP_NAME} "
            f"--restart always "
            f"-p 80:8000 "
            f"--env-file /home/{USER}/{APP_NAME}/.env "
            f"{APP_NAME}"
        )
        if run_command(ssh, cmd_run, sudo=True):
            print("\n--- Deployment Complete! ---")
            print(f"App available at http://{HOST}")
        else:
            print("Failed to start container.")

    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        if ssh: ssh.close()

if __name__ == "__main__":
    main()
