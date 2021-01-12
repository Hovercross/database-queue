""" Models for database queue """

from django.db import models


class Job(models.Model):
    """ A job that needs to be run """

    func = models.CharField(max_length=4096)
    queued_at = models.DateTimeField(auto_now_add=True)
    priority = models.PositiveSmallIntegerField(default=1000)
    delay_until = models.DateTimeField(null=True)

    def __str__(self):
        return self.func


class JobArg(models.Model):
    """ A single argument for a job that needs to be run """

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    position = models.PositiveSmallIntegerField()
    arg = models.JSONField()

    class Meta:
        unique_together = (("job", "position"),)

    def __str__(self):
        return str(self.position)


class JobKWArg(models.Model):
    """ A keyword argument to a job """

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    param_name = models.CharField(max_length=255)
    arg = models.JSONField()

    def __str__(self):
        return self.param_name


class JobResult(models.Model):
    """ The result of a run job """

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    success = models.BooleanField()
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField()
    exception = models.TextField()
    traceback = models.TextField()
    result = models.JSONField(null=True)

    def __str__(self):
        return f"{self.job}: {self.success and 'Success' or 'Failure'}"
