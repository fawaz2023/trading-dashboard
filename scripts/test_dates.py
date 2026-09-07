from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time

options = Options()
driver = webdriver.Chrome(options=options)
driver.get('https://www.bseindia.com/corporates/ann.html?scrip=532955')
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'corporates-ann-from-date')))
time.sleep(2)

# Set dates
driver.execute_script('''
    var el = document.getElementById('corporates-ann-from-date');
    el.value = '01/01/2024';
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
''')
time.sleep(1)
btn = driver.find_element(By.XPATH, "//input[@id='btnSubmit' and @type='submit']")
driver.execute_script('arguments[0].click();', btn)
time.sleep(4)

rows = driver.find_elements(By.XPATH, '//tr')
for r in rows[:10]:
    print(r.text.replace('\n', ' '))
driver.quit()
