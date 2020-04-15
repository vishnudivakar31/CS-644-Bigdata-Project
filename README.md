### CS: 644: Introduction to Big Data, Flight Data Analysis

#### Design, implement, and run an Oozie workflow to find out
  - the 3 airlines with the highest and lowest probability, respectively, for being on
    schedule;
  - the 3 airports with the longest and shortest average taxi time per flight (both in and
    out), respectively; and
  - the most common reason for flight cancellations.

#### Project Requirements
  - Your workflow must contain at least three MapReduce jobs that run in fully distributed
    mode.
  - Run your workflow to analyze the entire data set (total 22 years from 1987 to 2008) at one
    time on two VMs first and then gradually increase the system scale to the maximum allowed
    number of VMs for at least 5 increment steps, and measure each corresponding workflow
    execution time.
  - Run your workflow to analyze the data in a progressive manner with an increment of 1 year,
    i.e. the first year (1987), the first 2 years (1987-1988), the first 3 years (1987-1989), …, and
    the total 22 years (1987-2008), on the maximum allowed number of VMs, and measure each
    corresponding workflow execution time.
