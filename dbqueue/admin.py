""" Admin helpers for database queue """

from django.contrib import admin

from . import models


@admin.register(models.Job)
class JobAdmin(admin.ModelAdmin):
    """ Job admin """

    list_filter = ["final_result__success"]
