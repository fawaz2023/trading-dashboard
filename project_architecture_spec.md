# Project Architecture: SBIA Terminal (Trading Dashboard)

**Document Version:** 1.1 (August 2026)
**Project Intent:** To build a proprietary, institutional-grade Indian market screener that tracks "smart money" footprints in the NSE and BSE. The system detects anomalous institutional accumulation (high delivery volumes, price compression, whale density) before retail participants notice, filtering out noise using multi-timeframe progressive metrics and Machine Learning, and displaying actionable entry/exit signals for trend-following and swing trading.

---

## 1. The Core Data Pipeline (The Engine)

The pipeline is the foundation. It runs entirely independent of any user interface.

```mermaid
flowchart TD
    %% Define styles
    classDef script fill:#1e293b,stroke:#818cf8,stroke-width:2px,color:#fff
    classDef data fill:#0f172a,stroke:#34d399,stroke-width:2px,color:#fff
    classDef engine fill:#3b0764,stroke:#d8b4fe,stroke-width:2px,color:#fff

    A([Windows Task Scheduler<br>7:15 PM Daily]) --> B
    B["auto_update_daily.bat<br>(Orchestrator)"]:::script --> C
    
    subgraph Data Ingestion
    C["auto_update_smart.py"]:::script
    C --> D1[(NSE Bhavcopy + Delivery)]
    C --> D2[(BSE Bhavcopy + Delivery)]
    end

    subgraph Progressive Metrics
    D1 & D2 --> E["Rolling Window Engine<br>(1W, 1M, 3M Averages)"]:::engine
    E --> F1[Delivery Percentage]
    E --> F2[Whale Density Ratio]
    E --> F3[ATW & Price Compression]
    end

    F1 & F2 & F3 --> G[("combined_dashboard_live.csv<br>(5,500+ Stocks)")]:::data
```

### 1.1 Automated Data Ingestion
*   **Trigger:** Windows Task Scheduler runs `auto_update_daily.bat`.
*   **Orchestrator:** `auto_update_smart.py` (v4).
*   **Execution:** Downloads daily Bhavcopy and Delivery data, handles missing exchange uploads with retries.

### 1.2 Progressive Metrics Calculation
The raw data is merged and passed through a rolling window engine to calculate proprietary "Smart Money" metrics (Whale Density, ATW, Delivery Turnover). Output is compiled into `combined_dashboard_live.csv`.

---

## 2. The Intelligence Layer & Simulation

```mermaid
flowchart LR
    %% Define styles
    classDef file fill:#0f172a,stroke:#34d399,stroke-width:2px,color:#fff
    classDef logic fill:#1e293b,stroke:#818cf8,stroke-width:2px,color:#fff
    
    A[("combined_dashboard_live.csv")]:::file --> B
    A --> C
    
    subgraph Signal Generation
    B["Legacy Alpha Engine<br>(calculate_active_signals.py)"]:::logic
    C["FlexGate 2.0 AI<br>(flexgate_2_scanner.py)"]:::logic
    end
    
    B -->|12-Condition Rules| D[("active_signals_ranked.csv")]:::file
    C -->|Random Forest ML| E[("sbia_flexgate_watchlist.csv")]:::file
    
    D --> F
    E --> F
    
    subgraph Paper Trading
    F["ledger_manager.py<br>(Paper Trading Engine)"]:::logic
    F -->|Checks Daily TP/SL| G1[("sbia_ledger.csv")]:::file
    F -->|Checks Chandelier Exits| G2[("flexgate2_ledger.csv")]:::file
    end
```

*   **Legacy Alpha Engine:** Uses a strict "12-Condition" algorithmic filter (ProgressiveSpiker).
*   **FlexGate 2.0 Engine:** Uses a Random Forest ML model (`flexgate2_model.joblib`) to evaluate `Whale_Density` and `STABILITY_SCORE`, generating 14-day ATR Chandelier Exits via Yahoo Finance.
*   **Ledger Manager:** Dynamically checks daily highs/lows against targets/stops. If hit, closes trade and logs P&L.

---

## 3. Legacy UI Architecture: Streamlit

The original frontend was built as a massive, single-file monolith. Because Streamlit is procedurally executed, every interaction forces the entire script to run from top to bottom.

```mermaid
flowchart TD
    %% Define styles
    classDef ui fill:#0f1117,stroke:#6366f1,stroke-width:2px,color:#fff
    classDef state fill:#052e16,stroke:#10b981,stroke-width:2px,color:#fff
    
    A["User Opens Browser"] --> B
    
    subgraph "dashboard_full.py (Monolith Execution)"
    B["Top of Script"]:::ui --> C["Load Custom CSS (HTML Injection)"]:::ui
    C --> D["Read CSV Files (Cached)"]:::ui
    D --> E{"st.tabs() Navigation"}:::ui
    
    E -->|Tab 1| F1["Legacy Screener UI"]:::ui
    E -->|Tab 2| F2["Alpha Engine UI"]:::ui
    E -->|Tab 3| F3["FlexGate Engine UI"]:::ui
    E -->|Tab 4| F4["Win Rate / Ledger UI"]:::ui
    end
    
    F1 & F2 & F3 & F4 --> G{"User Clicks Button or Sorts"}
    G -->|Triggers Rerun| B
    
    subgraph Session State
    H["st.session_state<br>(Remembers active tab during reruns)"]:::state
    end
    
    E -.-> H
```

### 3.1 Structural Limitations
*   **Execution Model:** Procedural. If you click a sort button on Tab 3, the entire file re-runs from Line 1.
*   **Styling:** Because Streamlit blocks custom styling, we use massive `unsafe_allow_html=True` blocks to brute-force CSS into the DOM.
*   **Data Grids:** Relies on `st.dataframe` which is rigid and hard to style dynamically.

---

## 4. Next-Gen UI Architecture: Plotly Dash

The Phase 2 frontend designed for public SaaS deployment, high performance, and premium aesthetics.

```mermaid
flowchart TD
    %% Define styles
    classDef route fill:#08090d,stroke:#fb7185,stroke-width:2px,color:#fff
    classDef component fill:#161922,stroke:#818cf8,stroke-width:2px,color:#fff
    
    subgraph "Shell Application (dash_app_v2.py)"
    A["Tailwind CSS CDN"]:::component
    B["Global Navigation Sidebar"]:::component
    C["dash.page_container<br>(Dynamic Router)"]:::component
    end
    
    A -.-> C
    B --> C
    
    subgraph "dash_pages/ (Isolated Modules)"
    C -->|Route: /| D1["dashboard.py<br>(Main Metrics)"]:::route
    C -->|Route: /institutional-signals| D2["institutional_signals.py<br>(Data Grid)"]:::route
    C -->|Route: /verify-conditions| D3["verify_conditions.py<br>(Stock Lookup)"]:::route
    end
    
    subgraph "Reactive State (No Page Reloads)"
    E["@callback (Input)"]:::component --> F["Python Logic"]:::component
    F --> G["Output (Updates Component only)"]:::component
    end
    
    D1 & D2 & D3 -.-> E
```

### 4.1 Reactive Routing
Unlike Streamlit, Dash separates the shell from the pages. `dash_app_v2.py` holds the persistent sidebar, and `dash.page_container` swaps out the middle content without ever reloading the browser.

### 4.2 Callback Engine
When a user types into the search box in `verify_conditions.py`, a `@callback` fires. It sends the input to Python, runs the logic, and returns *only* the new layout for that specific box. The rest of the page remains untouched. This is exponentially faster than Streamlit.

### 4.3 Styling System
*   **Tailwind:** Utility classes (`p-4 flex bg-primary`) generate CSS instantly.
*   **Glassmorphism Engine:** Centralized in `assets/style.css` using `backdrop-filter: blur()`.
*   **Typography:** Google Fonts (Inter + JetBrains Mono).
