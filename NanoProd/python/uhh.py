# coding: utf-8

import shutil
import threading

from RunKit.crabLaw import ProdTask, update_kinit_thread, cond
from RunKit.crabTask import Task as CrabTask
from RunKit.crabTaskStatus import Status


class UHHProdTask(ProdTask):

    def run(self):
        thread = threading.Thread(target=update_kinit_thread)
        thread.start()
        try:
            work_area, grid_job_id, done_flag = self.branch_data
            task = CrabTask.Load(workArea=work_area)
            if grid_job_id == -1:
                if task.taskStatus.status in [Status.CrabFinished, Status.PostProcessingFinished]:
                    # skipped preprocessing here
                    self.output().touch()
                else:
                    raise RuntimeError(f"task {task.name} is not ready for post-processing")
            else:
                print(f'Running {task.name} job_id = {grid_job_id}')
                job_home, remove_job_home = self.law_job_home()
                result = task.runJobLocally(grid_job_id, job_home)
                state_str = 'finished' if result else 'failed'
                if remove_job_home:
                    shutil.rmtree(job_home)
                with self.output().open('w') as output:
                    output.write(state_str)
        finally:
            cond.acquire()
            cond.notify_all()
            cond.release()
            thread.join()
