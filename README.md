# CalibViz
New python-based (Dash) calibration visualizer. This is currently being developed to support SAN airport model calibration.

This reorganized version introduces a clean multi-page architecture using the Dash Pages framework.

🗂️ Folder Structure
calibviz_app/
│
├── app.py                  # Creates Dash app, sets layout, defines global callbacks
├── index.py                # Entry point to launch the app
│
├── data/
│   ├── data_loader.py      # Loads environment, survey, and model data
│   └── store.py            # Stores loaded data (shared across pages)
│
├── layouts/
│   ├── navbar.py           # Top navigation bar with scenario selector
│   ├── sidebar.py          # Collapsible sidebar with mode buttons
│   └── summary_card.py     # Generates summary trip statistics table
│
├── pages/
│   ├── summary.py          # Aggregated tour type page
│   ├── tour_type.py        # Disaggregated tour type page
│   └── employee.py         # Employee trip page
│
└── utils/
    ├── constants.py        # Global constants and color sequences
    └── callbacks_common.py # Shared helper functions


## Setup Instructions
1. Install [UV](https://docs.astral.sh/uv/getting-started/installation/).
2. Clone this repository and navigate to it.
3. Create a uv virtual environment with python 3.11:
	```
	uv venv --python 3.11
	```
4. Install dependencies:
	```
	uv pip install -r requirements.txt
	```
5. Copy `.env.example` to `.env` and update configurations as needed.
6. Start the application:
	```sh
	uv run index.py
	```
	The app will start locally at:
	👉 http://127.0.0.1:8050/
