## Linear programming solver
library(lpSolveAPI)

## Simplification:
## 1 TB as 1000 GB and  1 GB as 1000 MB


## Goal: minimize total cost
## Decision variables: buy HDD or SSD

## Objective function:
## argmin(1*HDD+3*SSD) 

## Constraint without rebalance (units in TB, day):
##        4*HDD +    2*SSD >= 0.5*num_day
##    1.728*HDD + 4.32*SSD >= 0.05*num_day
##          HDD            >= 0
##                     SSD >= 0
##          HDD +      SSD  = 12*num_server

## Constraint with rebalance
##        4*HDD +    2*SSD >= 0.5*num_day
##    1.728*HDD + 4.32*SSD >= 0.05*num_day+rebalance
##          HDD            >= 0
##                     SSD >= 0
##          HDD +      SSD  = 12*num_server

## We can write rebalance as an IO bandwith requirement:
## If the servers need to be rebalanced, they don't provide
## their full IO bandwidth, interpret rebalancing as  occupying 
## IO bandwidth (function below is a little bit
## simplistic, as it assumes that all previous drives on the
## server were either HDD or SSD)
rebalance <- function(num_server,stored_data) stored_data/num_server


## Get the mix of HDDs and SSDs which minimize the cost equation
##
## Arguments:
##        num_day : numeric, denotes the number of days
##        num_server: numeric, denotes the number of servers
##        rebalance_data : boolean, if TRUE: rebalance when new server
##                                     is added to the network
##                                  if FALSE: do not rebelance when
##                                      server is added to the
##                                       network, useful only for
##                                       checking the impact of rebalancing
##        price: vector of length 2, denotes the price of HDD and SSD
##               drives in this order
## Output:
##       solved: "yes"/"no" whether the LP problem reached solution
##       opt_mix: number of HDD and SSD drives in the optimal
##                solution   
##       num_server: number of servers
##       max_storage: available storage capacity for new data
##                    (rebalancing reduced)
##       max_IOP: IOP TB/day (a bit artificial but it was easier to
##                            set up in units of TB and day)
get_mix <- function(num_day,num_server,rebalance_data,price) {
    stored_data <- 0.5*num_day
    read_data <- 0.05*num_day
    hdd_price <- price[1]
    ssd_price <- price[2]
    ## Initialize lpsolve, 5 constraints, 2 decision variables
    lp <- make.lp(5, 2)
    ## make constraints matrix
    set.column(lp, 1, c(4, 1.728, 1,0,1))
    set.column(lp, 2, c(2, 4.32, 0,1,1))
    ## make RHS
    IO_usage <- ifelse(rebalance_data==TRUE,
                       yes=read_data+stored_data/num_server,
                        no=read_data)
    set.rhs(lp, c(stored_data, IO_usage,
                  0,0,12*num_server))
    ## make signs for RHS
    set.constr.type(lp, c(rep(">=", 4),"="))
    ## make objective function
    set.objfn(lp, c(hdd_price, ssd_price))
    ## we can only buy integer number of disks
    set.type(lp, 1:2, "integer")
    solve(lp)
    return(list("solved"=ifelse(solve(lp)==0,"TRUE","FALSE"),
                "opt_mix"=get.variables(lp),
                "cost"=get.objective(lp),
                "num_server"=num_server,
                "storage"=list("required"=stored_data,
                               "available"=get.constraints(lp)[1]),
                "IO_bandwidth"=list("required"=
                                       IO_usage,
                                    "available"=
                                        get.constraints(lp)[2])))
}

get_opt_mix <- function(num_day,rebalance_data=TRUE,price=c(1,3)) {
    solved <- FALSE
    num_server <- 1
    ## find minimal number of servers
    while(solved==FALSE) {
        num_server <- num_server+1
        solved <- get_mix(num_day,num_server,
                          rebalance_data,price)$solved
    }
    ## check if it is really cheaper than alternatives with
    ## more servers (max three times the num_servers as HDDs
    ## have twice as large storage space but SSDs are 2.5 times
    ## as fast, and 3*num_server is an integer) 
    prices <-    vapply(FUN=function(num_day,num_server,
                                     rebalance_data,price){
                       get_mix(num_day,num_server,
                       rebalance_data,price)$cost
                      },
                      X=num_server:(3*num_server),
                      num_day=num_day,
                      price=price,rebalance_data=rebalance_data,
                      FUN.VALUE=numeric(1))
   ## find cheapest option
    cheapest_option <- get_mix(num_day,num_server=
                    (num_server:(3*num_server))[which.min(prices)],
                      rebalance_data=rebalance_data,price=price)
    return(cheapest_option)
}

## ------------------------ run ------------------ ##

## Optimal solutions after 6 months
  ## assume no rebalancing for the sake of comparison
         ## 2 servers with 24 HDDs total
get_opt_mix(num_day=182,rebalance_data=FALSE)
  ## including rebalancing when a new server is added
  ## increases the required number of servers from 2 to 3
          ## 3 servers with 36 HDDs total

get_opt_mix(num_day=182,rebalance_data=TRUE)
  ## to get an optimal mix of HDDs and SSDs when SSD is not 0,
  ## we can increase the price of HDDs
       ## 3 servers with 10 HDDs and 26 SSDs total
       ## note that after 6 months, the data storage requirement
       ## is 91 TB and this solution provides 92 TB
       ## storage capacity
get_opt_mix(num_day=182,rebalance_data=TRUE,price=c(4,3))

## Optimal solutions after a year
  ## assume no rebalancing for the sake of comparison
        ## 4 servers with 48 HDDs total
get_opt_mix(num_day=365,rebalance_data=FALSE)
  ## including rebalancing does not increase the number of
  ## servers this time
       ##  4 servers with 48 HDDs total       
get_opt_mix(num_day=365,rebalance_data=TRUE)



