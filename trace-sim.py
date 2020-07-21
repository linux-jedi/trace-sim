#!/usr/bin/env python3

import click

import numpy as np
import pandas as pd

'''
Constants determined from microbenchmarks
'''
ELIGIBLE_JOBS_RATIO = 0.1
PRECOMPUTE_TIME_RATIO = .05
AVERAGE_SPEEDUP = 1.07

@click.command()
@click.option('--output', default='out.csv', help="Name of the file output should be written to")
@click.argument('trace')
def sim(output, trace):
    # Setup input dataframe
    input_columns = [
        'id',
        'priority',
        'scheduling_class',
        'submission_time',
        'schedule_time',
        'finish_time',
        'exec_time'
    ]
    input_df = pd.read_csv(trace)

    # Setup logging dataframe
    log_columns = [
        'id',
        'can_precompute',
        'exec_time',
        'precompute_exec_time',
        'precompute_time',
        'wait_time',
        'priority'
        ]
    logging_df = pd.DataFrame([], columns=log_columns)

    # Execution simulation
    result = execute(input_df)
    print(result)

    # Write to CSV
    output_logs(result, output)


def output_logs(df, filename):
    df.to_csv(filename)

def execute(jobs_df):
    return jobs_df.apply(execute_job, axis=1)

def execute_job(job):
    '''
    Copy over job info to output log
    '''
    exec_log = dict()
    exec_log["id"] = job['id']
    exec_log["scheduling_class"] = job['scheduling_class']
    exec_log["priority"] = job['priority']

    '''
    1. Check if we can precompute
    
    This will end up depending on power grid carbon intensity, the time
    shiftability of the job, and the effectiveness of precomputation on the
    current job type.
    '''
    if not is_precomputable(job):
        exec_log["can_precompute"] = 0
        return pd.Series(exec_log)
    exec_log["can_precompute"] = 1

    '''
    2. Calculate wait time
    '''
    exec_log["wait_time"] = 5

    '''
    3. Calculate precompute time
    '''
    exec_log["precompute_time"] = calculate_precompute_time(job)

    '''
    4. Calculate time saved for precomputation
    '''
    speedup = calculate_speedup(job)

    '''
    5. Calculate new execution time
    '''
    exec_log["precompute_exec_time"] = job['exec_time'] / speedup

    return pd.Series(exec_log)

'''
TODO: move these functions to own file/module to separate precompute
calculations/logic from trace execution logic

Determine if we can precompute a job. For now just precompute on
the percentage of jobs our microbenchmarks tell us are eligible.
'''
def is_precomputable(job):
    rand_draw = np.random.randint(0, 100)
    return rand_draw < ELIGIBLE_JOBS_RATIO * 100


def calculate_precompute_time(job):
    return PRECOMPUTE_TIME_RATIO * job['exec_time']

def calculate_speedup(job):
    return AVERAGE_SPEEDUP

if __name__ == '__main__':
    sim()