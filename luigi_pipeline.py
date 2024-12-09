import luigi
import os
import wget
import tarfile
import pandas as pd
import shutil
import logging
import gzip


class DownloadDatasetTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data_downloads')

    def output(self):
        return luigi.LocalTarget(os.path.join(self.download_dir, f"{self.dataset_name}_RAW.tar"))

    def run(self):
        os.makedirs(self.download_dir, exist_ok=True)
        url = f"ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE68nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        logging.info(f"Downloading dataset from: {url}")
        wget.download(url, out=self.output().path)
        logging.info(f"Dataset downloaded to: {self.output().path}")


class ExtractDatasetTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data_downloads')
    extract_dir = luigi.Parameter(default='data_extracted')

    def requires(self):
        return DownloadDatasetTask(self.dataset_name, self.download_dir)

    def output(self):
        return luigi.LocalTarget(self.extract_dir)

    def run(self):
        os.makedirs(self.extract_dir, exist_ok=True)
        with tarfile.open(self.input().path, 'r') as tar:
            tar.extractall(path=self.extract_dir)
            logging.info(f"Files extracted to: {self.extract_dir}")


class ProcessDatasetTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    extract_dir = luigi.Parameter(default='data_extracted')
    processed_dir = luigi.Parameter(default='data_processed')

    def requires(self):
        return ExtractDatasetTask(self.dataset_name)

    def output(self):
        return luigi.LocalTarget(self.processed_dir)

    def run(self):
        os.makedirs(self.processed_dir, exist_ok=True)
        for root, _, files in os.walk(self.extract_dir):
            for file in files:
                if file.endswith('.txt.gz'):
                    file_path = os.path.join(root, file)
                    self._decompress_file(file_path)

    def _decompress_file(self, file_path):
        with gzip.open(file_path, 'rt') as gz_file:
            content = gz_file.read()

        output_file_path = os.path.join(
            self.processed_dir,
            os.path.basename(file_path).replace('.gz', '_processed.txt')
        )
        with open(output_file_path, 'w') as out_file:
            out_file.write(content)

        logging.info(f"Decompressed and saved: {output_file_path}")


class TrimDatasetColumnsTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    processed_dir = luigi.Parameter(default='data_processed')
    trimmed_dir = luigi.Parameter(default='data_trimmed')

    def requires(self):
        return ProcessDatasetTask(self.dataset_name)

    def output(self):
        return luigi.LocalTarget(self.trimmed_dir)

    def run(self):
        os.makedirs(self.trimmed_dir, exist_ok=True)
        columns_to_remove = ['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function']
        for root, _, files in os.walk(self.processed_dir):
            for file in files:
                if file.endswith('_processed.txt'):
                    file_path = os.path.join(root, file)
                    self._trim_columns(file_path, columns_to_remove)

    def _trim_columns(self, file_path, columns_to_remove):
        df = pd.read_csv(file_path, sep='\t')
        trimmed_df = df.drop(columns=columns_to_remove, errors='ignore')

        output_file_path = os.path.join(
            self.trimmed_dir,
            os.path.basename(file_path).replace('_processed.txt', '_trimmed.txt')
        )
        trimmed_df.to_csv(output_file_path, sep='\t', index=False)
        logging.info(f"Trimmed and saved: {output_file_path}")


class CleanupTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    cleanup_dirs = luigi.ListParameter(default=['data_downloads', 'data_extracted'])

    def requires(self):
        return TrimDatasetColumnsTask(self.dataset_name)

    def output(self):
        return luigi.LocalTarget('cleanup_status.done')

    def run(self):
        for directory in self.cleanup_dirs:
            if os.path.exists(directory):
                shutil.rmtree(directory)
                logging.info(f"Cleaned directory: {directory}")

        with self.output().open('w') as cleanup_file:
            cleanup_file.write("Cleanup completed.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    luigi.run()
