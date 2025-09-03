# CalibViz
New python-based (Dash) calibration visualizer. This is currently being developed to support SAN airport model calibration.

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
	uv run app.py
	```
