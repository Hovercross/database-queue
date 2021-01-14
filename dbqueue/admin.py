""" Admin helpers for database queue """

from django.contrib import admin

from . import models


@admin.register(models.Job)
class JobAdmin(admin.ModelAdmin):
    """ Job admin """

    list_filter = ["final_result__success"]
    list_display = ["id", "__str__", "queued_at", "priority", "finished"]

    def finished(self, obj: models.Job):
        if not obj.final_result:
            return None

        return obj.final_result.success

    finished.boolean = True
