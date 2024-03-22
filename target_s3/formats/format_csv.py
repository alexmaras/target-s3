import csv
import io

from target_s3.formats.format_base import FormatBase

class FormatCsv(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "csv")
        pass

    def _prepare_records(self):
        return super()._prepare_records()

    def _write(self) -> None:
        output = io.StringIO()
        available_keys = list()
        for row in self.records:
            for key in row.keys():
                if key not in available_keys:
                    available_keys.append(key)
        writer = csv.DictWriter(output, quoting=csv.QUOTE_NONE, fieldnames=available_keys)
        writer.writeheader()
        for row in self.records:
            writer.writerow(row)
        return super()._write(output.getvalue())

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
