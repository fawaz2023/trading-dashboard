import os
files = ['auto_update_smart.py', 'calculate_active_signals.py']

injection = '''import sys
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
'''

for fn in files:
    with open(fn, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if "sys.stdout = io.TextIOWrapper" not in content:
        # insert right after import os or at top
        if "import os" in content:
            content = content.replace("import os", "import os\n" + injection, 1)
        else:
            content = injection + "\n" + content
            
        with open(fn, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Patched {fn}")
