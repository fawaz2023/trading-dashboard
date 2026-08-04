with open('dashboard_full.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

in_data_health = False
new_lines = []

for line in lines:
    if 'elif page == "Data Health":' in line:
        in_data_health = True
        new_lines.append(line)
        continue
    
    if in_data_health and 'elif page == "Signals":' in line:
        in_data_health = False
        new_lines.append(line)
        continue
        
    if in_data_health:
        if 'tabs = st.tabs' in line:
            new_lines.append('    with st.expander("🔍 Inspect Raw Data Files", expanded=False):\n')
            new_lines.append('    ' + line)
        elif 'Data Health & Download' in line:
            new_lines.append(line)  # Do not indent the main title
        else:
            if line.strip() == '':
                new_lines.append('\n')
            else:
                new_lines.append('    ' + line)
    else:
        new_lines.append(line)

with open('dashboard_full.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('Expander indentation applied.')
