import paramiko

HOST = "172.168.10.106"
USER = "warehouseai"
PASS = "Ecosat201."

def main():
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(f"Connecting to {HOST}...")
        ssh.connect(HOST, username=USER, password=PASS)
        
        print("\n--- Docker Status ---")
        stdin, stdout, stderr = ssh.exec_command("docker ps -a")
        print(stdout.read().decode())
        
        print("\n--- Container logs (tail) ---")
        stdin, stdout, stderr = ssh.exec_command("docker logs --tail 20 duma_planta")
        print(stdout.read().decode())
        print(stderr.read().decode()) # logs often go to stderr
        
        print("\n--- Local Curl Test ---")
        stdin, stdout, stderr = ssh.exec_command("curl -I http://localhost:80")
        print(stdout.read().decode())
        print(stderr.read().decode())

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if ssh: ssh.close()

if __name__ == "__main__":
    main()
