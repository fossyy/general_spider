import json
import os

class JsonWriterPipeline:
    def open_spider(self, spider):
        output_file = getattr(spider, 'output_file', None)
        if not output_file:
            raise ValueError('output_file must be specified')

        self.output_file = output_file
        self.first_item = True

        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            with open(output_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()

            if content.endswith(']'):
                content = content[:-1]
                if content.strip()[-1] == ',':
                    content = content[:-1]

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)
                self.first_item = False
        else:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('[')

    def close_spider(self, spider):
        with open(self.output_file, 'a', encoding='utf-8') as f:
            f.write(']')

    def process_item(self, item, spider):
        with open(self.output_file, 'a', encoding='utf-8') as f:
            if not self.first_item:
                f.write(',\n')
            else:
                self.first_item = False

            line = json.dumps(dict(item), indent=None)
            f.write(line)
        return item