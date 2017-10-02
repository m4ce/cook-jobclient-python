def generate_batch_request(jobs, batch_size):
    """Create a batch request by slicing up a given list of jobs

    Args:
        jobs (list): List of jobs
        batch_size (int): Size of each batch

    Returns:
        list: The generated batch request
    """
    # build a single request
    batch = list()
    for i in range(0, len(jobs), batch_size):
        chunk = jobs[i:i + batch_size]
        batch.append(["job={}".format(uid) for uid in chunk])

    return batch

