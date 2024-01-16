#

import rich
import rich.progress


def add_tasks(
    progress: rich.progress.Progress,
    task: rich.progress.TaskID,
    tasks: int,
) -> None:
    total = progress.tasks[task].total

    if total is None:
        total = 0

    progress.update(task, total=total + tasks)
