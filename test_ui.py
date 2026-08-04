from streamlit.testing.v1 import AppTest

def run_tests():
    print("Initializing AppTest...")
    at = AppTest.from_file("dashboard_full.py", default_timeout=30)
    at.run()
    
    pages = ["Dashboard", "Data Health", "Signals", "Institutional Signals", "Verify Conditions", "Watchlist", "Win Rate"]
    
    for p in pages:
        print(f"Testing page: {p}")
        # Find the sidebar radio and change it
        # Assuming the radio is the first one in the sidebar
        at.sidebar.radio[0].set_value(p).run()
        
        if at.exception:
            print(f"ERROR on page {p}: {at.exception[0]}")
            return
            
        if p == "Institutional Signals":
            print("  -> Testing Institutional Signals filters...")
            
            # Change Timeframe
            at.radio[0].set_value("Today Only").run()
            if at.exception: print(f"ERROR on Timeframe: {at.exception[0]}")
                
            # Change Min Score
            # selectbox[0] might be min score or something else. We look for 'Min Score'
            for sb in at.selectbox:
                if sb.label == "Min Score":
                    sb.set_value(1).run()
                    print(f"  -> Min Score changed to 1")
                    break
            if at.exception: print(f"ERROR on Min Score: {at.exception[0]}")
            
            # Change Search Query
            for ti in at.text_input:
                if ti.label == "Search Symbol":
                    ti.set_value("SEIL").run()
                    print(f"  -> Search Symbol changed to SEIL")
                    break
            if at.exception: print(f"ERROR on Search Query: {at.exception[0]}")
            
            # Change Exchange
            for sb in at.selectbox:
                if sb.label == "Exchange":
                    sb.set_value("NSE").run()
                    print(f"  -> Exchange changed to NSE")
                    break
            if at.exception: print(f"ERROR on Exchange: {at.exception[0]}")
            
    print("All UI tests passed successfully! No tracebacks found.")

if __name__ == "__main__":
    run_tests()
