from django.db import transaction
from django.utils import timezone

from ... import models, utils
from ...exceptions import NotHistoricalModelError
from . import populate_history

import time

class Command(populate_history.Command):
    args = "<app.model app.model ...>"
    help = (
        "Scans HistoricalRecords for identical sequencial entries "
        "(duplicates) in a model and deletes them."
    )

    DONE_CLEANING_FOR_MODEL = "Removed {count} historical records for {model}\n"

    def add_arguments(self, parser):
        parser.add_argument("models", nargs="*", type=str)
        parser.add_argument(
            "--auto",
            action="store_true",
            dest="auto",
            default=False,
            help="Automatically search for models with the HistoricalRecords field "
            "type",
        )
        parser.add_argument(
            "-d", "--dry", action="store_true", help="Dry (test) run only, no changes"
        )
        parser.add_argument(
            "-m", "--minutes", type=int, help="Only search the last MINUTES of history"
        )
        parser.add_argument(
            "--excluded_fields",
            nargs="+",
            help="List of fields to be excluded from the diff_against check",
        )
        parser.add_argument(
            "--batch-size", type=int, default=None, help="Run the command and do deletion in batches of batch_size, each of which will be a separate transaction",
        )
        parser.add_argument(
            "--batch-sleep", type=int, default=0, help="Number of seconds to wait in between batches",
        )

    def handle(self, *args, **options):
        self.verbosity = options["verbosity"]
        self.excluded_fields = options.get("excluded_fields")

        to_process = set()
        model_strings = options.get("models", []) or args

        if model_strings:
            for model_pair in self._handle_model_list(*model_strings):
                to_process.add(model_pair)

        elif options["auto"]:
            to_process = self._auto_models()

        else:
            self.log(self.COMMAND_HINT)

        self._process(to_process, date_back=options["minutes"], dry_run=options["dry"], batch_size=options["batch_size"], batch_sleep=options["batch_sleep"])

    def _process(self, to_process, date_back=None, dry_run=True, batch_size=None, batch_sleep=0):
        if date_back:
            stop_date = timezone.now() - timezone.timedelta(minutes=date_back)
        else:
            stop_date = None

        batch_mode = batch_size != None

        for model, history_model in to_process:
            m_qs = history_model.objects
            deletion_list = []
            if stop_date:
                m_qs = m_qs.filter(history_date__gte=stop_date)
            found = m_qs.count()
            self.log("{0} has {1} historical entries".format(model, found), 2)
            if not found:
                continue

            # Break apart the query so we can add additional filtering
            model_query = model.objects.all()

            # If we're provided a stop date take the initial hit of getting the
            # filtered records to iterate over
            if stop_date:
                model_query = model_query.filter(
                    pk__in=(m_qs.values_list(model._meta.pk.name).distinct())
                )

            for o in model_query.iterator():
                self._process_instance(o, model, stop_date=stop_date, dry_run=dry_run, batch_mode=batch_mode, deletion_list=deletion_list)

            if batch_mode and not dry_run:
                batch_count = (len(deletion_list)) // batch_size
                if (len(deletion_list)) % batch_size > 0:
                    batch_count += 1

                self.log(
                    "Starting deletion with {batch_count} batches of {batch_size} history records of {model}\n".format(batch_count=batch_count, batch_size=batch_size, model=model)
                )

                for batch_index in range(batch_count):
                    with transaction.atomic():
                        self.log(
                            "Starting batch {batch_index} of {batch_count} total batches\n".format(batch_index=batch_index, batch_count=batch_count)
                        )
                        batch_start = batch_size * batch_index
                        batch_end = min(batch_start + batch_size, len(deletion_list))
                        self.log(
                            "Batch will run from {batch_start} to {batch_end} of {total} total entries\n".format(batch_start=batch_start, batch_end=batch_end, total=len(deletion_list))
                        )
                        batch_ids_to_delete = deletion_list[batch_start:batch_end]
                        to_delete_qs = history_model.objects.filter(id__in=batch_ids_to_delete)
                        self.log(
                            "Deleting {to_delete} history records of {model}\n".format(to_delete=len(to_delete_qs), model=model)
                        )
                        to_delete_qs.delete()
                    if batch_index != batch_count - 1 and batch_sleep > 0:
                        self.log(
                            "Pause after batch {batch_index}, sleeping for {batch_sleep} seconds\n".format(batch_index=batch_index, batch_sleep=batch_sleep)
                        )
                        time.sleep(batch_sleep)

                self.log(
                    "Finished batch deletion for {model}\n".format(model=model)
                )

    def _process_instance(self, instance, model, stop_date=None, dry_run=True, batch_mode=False, deletion_list=[]):
        entries_deleted = 0
        history = utils.get_history_manager_for_model(instance)
        o_qs = history.all()
        if stop_date:
            # to compare last history match
            extra_one = o_qs.filter(history_date__lte=stop_date).first()
            o_qs = o_qs.filter(history_date__gte=stop_date)
        else:
            extra_one = None

        if batch_mode:
            f1 = o_qs.first()
            if not f1:
                return

            for f2 in o_qs[1:]:
                entries_deleted += self._check_and_batch(f1, f2, deletion_list)
                f1 = f2
            if extra_one:
                entries_deleted += self._check_and_batch(f1, extra_one, deletion_list)

            self.log(
                "Gathered {entries_deleted} records of {model} to delete\n".format(entries_deleted=entries_deleted, model=model)
            )
        else:
            with transaction.atomic():
                # ordering is ('-history_date', '-history_id') so this is ok
                f1 = o_qs.first()
                if not f1:
                    return

                for f2 in o_qs[1:]:
                    entries_deleted += self._check_and_delete(f1, f2, dry_run)
                    f1 = f2
                if extra_one:
                    entries_deleted += self._check_and_delete(f1, extra_one, dry_run)

            self.log(
                self.DONE_CLEANING_FOR_MODEL.format(model=model, count=entries_deleted)
            )

    def log(self, message, verbosity_level=1):
        if self.verbosity >= verbosity_level:
            self.stdout.write(message)

    def _check_and_delete(self, entry1, entry2, dry_run=True):
        delta = entry1.diff_against(entry2, excluded_fields=self.excluded_fields)
        if not delta.changed_fields:
            if not dry_run:
                entry1.delete()
            return 1
        return 0
    
    def _check_and_batch(self, entry1, entry2, deletion_list):
        delta = entry1.diff_against(entry2, excluded_fields=self.excluded_fields)
        if not delta.changed_fields:
            deletion_list.append(entry1.id)
            return 1
        return 0
