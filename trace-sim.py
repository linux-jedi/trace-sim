#!/usr/bin/env python3
import enum
import queue

import click

import numpy as np
import pandas as pd

'''
Constants determined from microbenchmarks
'''
class ParamTypes(enum.Enum):
    precomputable_ratio = 0
    precompute_time = 1
    precompute_speedup = 2

RETRIEVAL_TIME = 100

'''
TODO: Set these up as program paramaters
'''
WORKLOAD_TYPES = [
    'ml',
    'graphics',
    'map-reduce'
]

PARAMS = {
    'ml': (1.0, .05, 1.07),
    'graphics': (0.1, .05, 1.07),
    'map-reduce': (0.1, .05, 1.07)
}

PRIORITY_WEIGHT = 1.0
DEADLINE_WEIGHT = 1.0
ESTIMATE_WEIGHT = 1.0

'''
Enums for to describe jobs
'''
class JobFields(enum.Enum):
    id = 0
    priority = 1
    deadline = 2
    estimate = 3

'''
Dataframe columns
'''
LOG_COLUMNS = [
    'id',
    'priority',
    'can_precompute',
    'exec_time',
    'precompute_time',
    'retrieval_time',
    'wait_time'
    ]


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
        'exec_time',
        'workload_classification'
        ]
    input_df = pd.read_csv(trace)
    input_df['workload_classification'] = 0

    # Execution simulation
    result = execute(input_df)
    print(result)

    # Write to CSV
    output_logs(result, output)


def output_logs(df, filename):
    df.to_csv(filename)

def execute(jobs_df):
    # Setup logging dataframe
    log_df = pd.DataFrame([], columns=LOG_COLUMNS)
    log_df.set_index("id", inplace=True)

    # Setup priority queue
    job_queue = queue.PriorityQueue()
    
    curr_time = 0
    curr_row = 0

    while curr_row < len(jobs_df.index):
        # Add job to queue if empty or not caught up to recently completed job
        while curr_row < len(jobs_df.index) and\
            (job_queue.empty() or jobs_df.iloc[curr_row]['submission_time'] < curr_time):
            # Get job data
            job_entry = jobs_df.iloc[curr_row]
            job_exec_time = job_entry['exec_time']

            # Setup logging for job
            log_entry = dict()
            log_entry['id'] = job_entry['id']

            # check if we can precompute
            if is_precomputable(job_entry):
                # Calculate speedup
                speedup_factor = calculate_speedup(job_entry)
                precompute_time = calculate_precompute_time(job_entry)
                job_exec_time = job_exec_time / speedup_factor

                # Log hit + retrieval time
                log_entry['precompute_time'] = precompute_time
                log_entry['retrieval_time'] = RETRIEVAL_TIME

            job = (
                job_entry['id'],
                job_entry['priority'],
                job_entry['deadline'],
                job_exec_time
            )

            # Calculate job priority
            priority = PRIORITY_WEIGHT * job[JobFields.priority.value] \
                + DEADLINE_WEIGHT * -job[JobFields.deadline.value] \
                + ESTIMATE_WEIGHT * job[JobFields.estimate.value]
            priority = -priority # negate, priority queue chooses smallest value
            log_entry['priority'] = priority
            
            # Add to queue
            print("Adding job to the queue")
            job_queue.put((priority, job))

            # Log
            log_df = log_df.append(log_entry, ignore_index=True)

            # Iterate 
            curr_row = curr_row + 1
        
        # Select job to execute
        _, job = job_queue.get()
        job_entry = jobs_df.loc[jobs_df['id'] == job[JobFields.id.value]].iloc[0]

        # Execute job
        print("Executing job: ", job[JobFields.id.value])
        if curr_time == 0:
            curr_time = jobs_df.iloc[curr_row]['submission_time']
        
        wait_time = curr_time - job_entry['schedule_time']
        curr_time = curr_time + job[JobFields.estimate.value]

        # Log completed job
        log_df.at[job_entry[JobFields.id.value], 'wait_time'] = wait_time

    return log_df

'''
TODO: move these functions to own file/module to separate precompute
calculations/logic from trace execution logic

Determine if we can precompute a job. For now just precompute on
the percentage of jobs our microbenchmarks tell us are eligible.
'''
def is_precomputable(job):
    workload_params = PARAMS[WORKLOAD_TYPES[job['workload_classification']]]

    rand_draw = np.random.randint(0, 100)
    return rand_draw <  workload_params[ParamTypes.precomputable_ratio.value] * 100


def calculate_precompute_time(job):
    workload_params = PARAMS[WORKLOAD_TYPES[job['workload_classification']]]
    return workload_params[ParamTypes.precompute_time.value] * job['exec_time']

def calculate_speedup(job):
    workload_params = PARAMS[WORKLOAD_TYPES[job['workload_classification']]]
    return workload_params[ParamTypes.precompute_speedup.value]

if __name__ == '__main__':
    sim()