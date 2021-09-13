# distributify

Steps:

1. Run: python app.py
2. Simulate crash: Press Ctrl-C before the execution finishes to simulate crash.
3. Resume job: In conf/config.yaml, uncomment the commented lines, and put the relative path of the output of step 1 (outputs/date/time).

Alternatively, python app.py hydra.run.dir=outputs/date/time should also work.