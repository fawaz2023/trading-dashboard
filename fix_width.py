import re
content = open('dashboard_full.py', 'r', encoding='utf-8').read()
content = re.sub(r'width=[\'\"]stretch[\'\"]', 'use_container_width=True', content)
open('dashboard_full.py', 'w', encoding='utf-8').write(content)
