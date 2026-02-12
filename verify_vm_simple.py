import paramiko

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."

def main():
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(HOST, username=USER, password=PASS)
        
        # Check running containers
        stdin, stdout, stderr = ssh.exec_command("docker ps --format '{{.ID}} {{.Status}} {{.Ports}}' --filter name=duma_planta")
        out = stdout.read().decode().strip()
        print(f"STATUS: {out}")
        
        # Check logs if not running
        if "Up" not in out:
            print("Container not running. Logs:")
            stdin, stdout, stderr = ssh.exec_command("docker logs --tail 20 duma_planta")
            print(stdout.read().decode())
            print(stderr.read().decode())
        else:
            print("Container is running.")
            # Verify internal curl
            stdin, stdout, stderr = ssh.exec_command("curl -I http://127.0.0.1:80")
            print(f"CURL: {stdout.read().decode()}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if ssh: ssh.close()

if __name__ == "__main__":
    main()
