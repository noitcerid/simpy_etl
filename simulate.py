import simpy
import random
import statistics

wait_times = []
PRINT_VERBOSE = True


class Importer(object):
    def __init__(self, env, num_file_nodes, num_post_import_nodes):
        self.env = env
        self.file_node = simpy.Resource(env, num_file_nodes)
        self.post_import_node = simpy.Resource(env, num_post_import_nodes)

    def import_file_to_staging(self, file):
        import_time = random.randint(1, 10)
        yield self.env.timeout(import_time)  # Files generally take 1-10 seconds
        if PRINT_VERBOSE:
            print(f"File {file} import executed in {import_time} seconds")

    def execute_post_import_task(self, file):
        task_time = random.randint(2, 8)
        yield self.env.timeout(task_time)  # Tasks generally take 2-8 seconds
        if PRINT_VERBOSE:
            print(f"Post task for file {file} executed in {task_time} seconds")


def start_file_import(env, file, importer):
    # File is dropped into folder
    drop_time = env.now

    with importer.file_node.request() as request:
        yield request
        yield env.process(importer.import_file_to_staging(file))

    if random.choice([True, False]):
        with importer.post_import_node.request() as request:
            yield request
            yield env.process(importer.execute_post_import_task(file))

    # File import is complete
    wait_times.append(env.now - drop_time)


def run_importer(env, num_file_nodes, num_post_import_nodes, num_files):
    importer = Importer(env, num_file_nodes, num_post_import_nodes)

    # Set initial files waiting for import
    print("Loading initial file queue of 2 files")
    for file in range(1, 2):
        env.process(start_file_import(env, file, importer))

    while True and file < num_files:  # num_files is necessary to ensure there is a break in loop
        yield env.timeout(5)  # Wait is approx. 5 minutes between file imports

        file += 1
        env.process(start_file_import(env, file, importer))


def get_average_wait_time(wait_times):
    average_wait = statistics.mean(wait_times)

    minutes, frac_minutes = divmod(average_wait, 1)
    seconds = frac_minutes * 60
    if seconds == 60:
        minutes += 1
        seconds = 0
    return round(minutes), round(seconds)


def get_user_input():
    num_file_nodes = input("How many file import nodes would you like to simulate? ")
    num_post_import_nodes = input("How many post import nodes would you like to simulate? ")
    num_files = input("How many file imports should be simulated? ")

    params = [num_file_nodes, num_post_import_nodes, num_files]

    if all(str(i).isdigit() for i in params):
        params = [int(x) for x in params]
    else:
        print(
            "Could not parse input... default of 1 node each and 10 files will be used."
        )
        params = [1, 1, 10]

    return params


def main():
    # Set up simulation
    random.seed(42)
    num_file_nodes, num_post_import_nodes, num_files = get_user_input()
    sim_mode = input("Run in real-time (rt) or simulated (sim)? ")

    env = simpy.Environment()

    if sim_mode.lower() == 'rt':
        speed = input("What speed factor (1 = 1 sec. = real-time)? ")
        if speed == '' or speed is None:
            speed = 0.1
        env = simpy.rt.RealtimeEnvironment(factor=float(speed))

    # Run simulation
    print(f"Starting simulation over {num_files} files...")
    env.process(run_importer(env, num_file_nodes, num_post_import_nodes, num_files))
    env.run()  # Set to run over 1 day (1440 minutes)

    # View Results
    mins, secs = get_average_wait_time(wait_times)
    print(
        "Simulation Complete!",
        f"\nThe average file import time is {mins} minutes and {secs} seconds."
    )


if __name__ == '__main__':
    main()
