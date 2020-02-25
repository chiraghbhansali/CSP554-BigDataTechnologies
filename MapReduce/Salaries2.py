from mrjob.job import MRJob

class MRSalaries(MRJob):

    def mapper(self, _, line):
        (name,jobTitle,agencyID,agency,hireDate,annualSalary,grossPay) = line.split('\t')
        annualSal=float(annualSalary)
        if annualSal<=49999.99:
            yield "Low", 1
        elif annualSal>49999.99 and annualSal<=99999.99:
            yield "Medium", 1
        else:
            yield "High", 1

    def combiner(self, SalaryGrp, counts):
        yield SalaryGrp, sum(counts)

    def reducer(self, SalaryGrp, counts):
        yield SalaryGrp, sum(counts)


if __name__ == '__main__':
    MRSalaries.run()


