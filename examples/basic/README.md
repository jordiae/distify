# Basic example

## Download

    git clone https://github.com/jordiae/distify
    mv distify/examples/basic/ .
    rm -rf distify/
    cd basic

## Setup

    python -m venv venv
    source venv/bin/activate
    python -m pip install -r requirements.txt

## Run

    python app.py

## Resume interrupted execution

    python app.py hydra.run.dir=outputs/2021-09-14/08-54-10
