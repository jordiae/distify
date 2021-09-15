# distributify

Steps:

0. Install (requirements.txt)
1. cd example/basic
2. Run: python app.py
3. Simulate crash: Press Ctrl-C before the execution finishes to simulate crash.
4. Resume job: python my_app.py hydra.run.dir=outputs/2021-09-14/08-54-10 (where hydra.run.dir is the output of the execution to be resumed)
