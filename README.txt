#################################################################################################
                                           PURPOSE
#################################################################################################
The purpose of this program is reading a file with taxi trips data, computing some KPIs for those 
trips and saving those KPIs in a JSON file. The computed KPIs are:
       - The average price per mile traveled by the customers of taxis
       - The distribution of payment types
       - The following custom indicator: (amount of tip + extra payment) / trip distance

The program also keeps a log of the data for which the KPIs have already been computed. It is
located in the output folder.

#################################################################################################
                                            USAGE
#################################################################################################
This program is designed to be run daily in a Linux server using cron or other service for 
automating tasks. It should be run daily specifying just the input and output folders so that 
each day the data of that day is computed.

#################################################################################################
                                        REQUIREMENTS
#################################################################################################
In order for the program to be run, the Python libraries specified in requirements.txt should
be installed in the system.
