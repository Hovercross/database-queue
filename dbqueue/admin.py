""" Admin helpers for database queue """

from django.contrib import admin
from django.db.models.query import QuerySet
from django.http.request import HttpRequest
from django.utils.translation import gettext_lazy as _

from . import models


class JobFinishedFilter(admin.SimpleListFilter):
    """ Simple filter to indicate if jobs have finished """

    title = _("job finished")
    parameter_name = "finished"

    def lookups(self, request, model_admin):
        return (
            ("success", _("Successfully")),
            ("failure", _("Unsuccessfully")),
            ("unknown", _("Not completed")),
        )

    def queryset(self, request, queryset):
        if self.value() == "success":
            return queryset.filter(final_result__success=True)

        if self.value() == "failure":
            return queryset.filter(final_result__success=False)

        if self.value() == "unknown":
            return queryset.filter(final_result__isnull=True)

        return queryset


class JobArgInline(admin.TabularInline):
    model = models.JobArg


class JobKWArgInline(admin.TabularInline):
    model = models.JobKWArg


class JobResultInline(admin.StackedInline):
    model = models.JobResult


@admin.register(models.Job)
class JobAdmin(admin.ModelAdmin):
    """ Job admin """

    inlines = [JobArgInline, JobKWArgInline, JobResultInline]

    list_filter = [JobFinishedFilter]
    list_display = ["id", "__str__", "queued_at", "priority", "finished"]

    def finished(self, obj: models.Job):
        if not obj.final_result:
            return None

        return obj.final_result.success

    finished.boolean = True

    def get_queryset(self, request: HttpRequest) -> QuerySet:
        return super().get_queryset(request).select_related("final_result")

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request, obj=None):
        return False
