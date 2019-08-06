import os
import luigi
import csv
from datetime import datetime, timedelta
from sniim.scrappers.agriculture import AgricultureMarketScrapper, BASE_CATEGORIES, BASE_SCHEMA


class ExtractSNIIMAgricultureInformation(luigi.Task):
    rundate = luigi.DateParameter(default=datetime.today())
    start_date = luigi.DateParameter(default=datetime.today())
    end_date = luigi.DateParameter(default=datetime.today())
    category = luigi.Parameter()
    base_path = luigi.Parameter(default='/home/emilio/sniim/')

    def output(self):
        sub_path = '{0}/raw/{1}/{2}.csv'.format(
            self.category[0].lower().replace(' ', '_'),
            self.rundate.strftime('%Y%m%d%M%s')[:12],
            self.start_date.strftime('%Y%m%d')
        )

        print(os.path.join(self.base_path, sub_path))
        return luigi.LocalTarget(
            os.path.join(self.base_path, sub_path)
        )

    def run(self):
        with self.output().open('w') as local_file:
            csv_writer = csv.DictWriter(local_file, fieldnames=BASE_SCHEMA)
            csv_writer.writeheader()

            scrapper = AgricultureMarketScrapper(
                category_url=self.category[1],
                prices_url=self.category[2]
            )

            for price_record in scrapper.scrape(start_date=self.start_date, end_date=self.end_date):
                print(price_record)
                csv_writer.writerow(price_record)


class ProcessAgricultureRawFile(luigi.Task):
    rundate = luigi.DateParameter(default=datetime.today())
    start_date = luigi.DateParameter(default=datetime.today())
    end_date = luigi.DateParameter(default=datetime.today())
    category = luigi.Parameter()
    base_path = luigi.Parameter(default='/home/emilio/sniim/')

    def requires(self):
        return ExtractSNIIMAgricultureInformation(
            rundate = self.rundate,
            start_date = self.start_date,
            end_date = self.end_date,
            category = self.category,
            base_path = self.base_path
        )

    def output(self):
        sub_path = '{0}/processed/{1}/{2}.csv'.format(
            self.category[0].lower().replace(' ', '_'),
            self.rundate.strftime('%Y%m%d%M%s')[:12],
            self.start_date.strftime('%Y%m%d')
        )

        return luigi.LocalTarget(
            os.path.join(self.base_path, sub_path)
        )

    def run(self):
        with self.input().open('r') as csv_raw_file:
            with self.output().open('w') as csv_processed_file:
                csv_processed_file.write(csv_raw_file.read())


class InsertAgricultureCategoryOnDB(luigi.Task):
    rundate = luigi.DateParameter(default=datetime.today())
    start_date = luigi.DateParameter(default=datetime.today())
    end_date = luigi.DateParameter(default=datetime.today())
    category = luigi.Parameter()
    base_path = luigi.Parameter(default='/home/emilio/sniim/')

    def requires(self):
        return ProcessAgricultureRawFile(
            rundate = self.rundate,
            start_date = self.start_date,
            end_date = self.end_date,
            category = self.category,
            base_path = self.base_path
        )

    def output(self):
        sub_path = '{0}/processed/{1}/SUCCESSDB'.format(
            self.category[0].lower().replace(' ', '_'),
            self.rundate.strftime('%Y%m%d%M%s')[:12],
            self.start_date.strftime('%Y%m%d')
        )

        return luigi.LocalTarget(
            os.path.join(self.base_path, sub_path)
        )

    def run(self):
        with self.output().open('w') as csv_processed_file:
            csv_processed_file.write('SUCCESS {}'.format(self.rundate))


class UpdateSNIIMInformation(luigi.Task):
    rundate = luigi.DateParameter(default=datetime.today())
    start_date = luigi.DateParameter(default=datetime.today())
    end_date = luigi.DateParameter(default=datetime.today())
    base_path = luigi.Parameter(default='/home/emilio/sniim/')
    category = luigi.Parameter()

    def requires(self):
        if not self.rundate:
            self.rundate = datetime.today()

        if not self.start_date:
            self.start_date = datetime.today()

        if not self.end_date:
            delta = timedelta(days=1)
            self.end_date = datetime.today() + delta


        return InsertAgricultureCategoryOnDB(
            rundate = self.rundate,
            start_date = self.start_date,
            end_date = self.end_date,
            category = self.category,
            base_path = self.base_path
        )

    def output(self):
        print(self.rundate.strftime('%Y%m%d%M%s')[:12])
        sub_path = '{0}/processed/{1}/FINAL'.format(
            self.category[0].lower().replace(' ', '_'),
            self.rundate.strftime('%Y%m%d%M%s')[:12],
            self.start_date.strftime('%Y%m%d')
        )

        return luigi.LocalTarget(
            os.path.join(self.base_path, sub_path)
        )

if __name__ == '__main__':
    luigi.build([
        UpdateSNIIMInformation(
            rundate = datetime.now(),
            start_date = datetime(2019, 7, 30),
            end_date = datetime(2019, 7, 31),
            category = BASE_CATEGORIES[0],
            base_path = '/Users/franciscovaquerociciliano/Develop/emilio/sniim'
        )
    ], local_scheduler=True)
