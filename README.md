# distify

Wrapper araound Ray for easy distributed processing in Python.

## Features

- Multiple backends: Ray, Multiprocessing, Multithreading, Sequential.
- Logging.
- Progress bar.
- Can run in local or in multiple nodes.
- Individual timeout for map applications.
- Resume from checkpointing.
- Hydra integration.

## Quickstart/usage

Install:

    pip install distify
  
and start from this example: https://github.com/jordiae/distify/tree/main/examples/basic

To run it:

    python app.py
  
To resume the execution (if interrupted):

    python app.py hydra.run.dir=outputs/2021-09-14/08-54-10
    
(where hydra.run.dir is the output of the execution to be resumed)
