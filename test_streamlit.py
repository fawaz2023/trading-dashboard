import subprocess
import time
import urllib.request

proc = subprocess.Popen(['python', '-m', 'streamlit', 'run', 'dashboard_full.py', '--server.headless', 'true', '--server.port', '8505'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(3)
try:
    urllib.request.urlopen('http://localhost:8505').read()
except Exception as e:
    print(e)
time.sleep(2)
proc.kill()
stdout, stderr = proc.communicate()
print('STDOUT:', stdout.decode('utf-8', errors='ignore'))
print('STDERR:', stderr.decode('utf-8', errors='ignore'))
