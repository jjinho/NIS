library(dplyr)


inc_by_dx <- function(df, icd9s) {
  if(as.numeric(df$YEAR) < 2009) {
    filter(df, 
             DX1  %in% icd9s |
             DX2  %in% icd9s |
             DX3  %in% icd9s |
             DX4  %in% icd9s |
             DX5  %in% icd9s |
             DX6  %in% icd9s |
             DX7  %in% icd9s |
             DX8  %in% icd9s |
             DX9  %in% icd9s |
             DX10 %in% icd9s |
             DX11 %in% icd9s |
             DX12 %in% icd9s |
             DX13 %in% icd9s |
             DX14 %in% icd9s |
             DX15 %in% icd9s)
  } else {
    filter(df, 
             DX1  %in% icd9s |
             DX2  %in% icd9s |
             DX3  %in% icd9s |
             DX4  %in% icd9s |
             DX5  %in% icd9s |
             DX6  %in% icd9s |
             DX7  %in% icd9s |
             DX8  %in% icd9s |
             DX9  %in% icd9s |
             DX10 %in% icd9s |
             DX11 %in% icd9s |
             DX12 %in% icd9s |
             DX13 %in% icd9s |
             DX14 %in% icd9s |
             DX15 %in% icd9s |
             DX16 %in% icd9s |
             DX17 %in% icd9s |
             DX18 %in% icd9s |
             DX19 %in% icd9s |
             DX20 %in% icd9s |
             DX21 %in% icd9s |
             DX22 %in% icd9s |
             DX23 %in% icd9s |
             DX24 %in% icd9s |
             DX25 %in% icd9s)
  }
}

inc_by_pr <- function(df, icd9s) {
  filter(df, 
           PR1  %in% icd9s |
           PR2  %in% icd9s |
           PR3  %in% icd9s |
           PR4  %in% icd9s |
           PR5  %in% icd9s |
           PR6  %in% icd9s |
           PR7  %in% icd9s |
           PR8  %in% icd9s |
           PR9  %in% icd9s |
           PR10 %in% icd9s |
           PR11 %in% icd9s |
           PR12 %in% icd9s |
           PR13 %in% icd9s |
           PR14 %in% icd9s |
           PR15 %in% icd9s)
}

exc_by_dx <- function(df, icd9) {
  if(as.numeric(df$YEAR) >= 2009) {
    filter(df, !DX1  %in% icd9 & 
               !DX2  %in% icd9 & 
               !DX3  %in% icd9 & 
               !DX4  %in% icd9 & 
               !DX5  %in% icd9 & 
               !DX6  %in% icd9 & 
               !DX7  %in% icd9 & 
               !DX8  %in% icd9 & 
               !DX9  %in% icd9 & 
               !DX10 %in% icd9 & 
               !DX11 %in% icd9 & 
               !DX12 %in% icd9 & 
               !DX13 %in% icd9 & 
               !DX14 %in% icd9 & 
               !DX15 %in% icd9 &
               !DX16 %in% icd9 & 
               !DX17 %in% icd9 & 
               !DX18 %in% icd9 & 
               !DX19 %in% icd9 & 
               !DX20 %in% icd9 & 
               !DX21 %in% icd9 & 
               !DX22 %in% icd9 & 
               !DX23 %in% icd9 & 
               !DX24 %in% icd9 & 
               !DX25 %in% icd9)
  } else {
    filter(df, !DX1  %in% icd9 & 
               !DX2  %in% icd9 & 
               !DX3  %in% icd9 & 
               !DX4  %in% icd9 & 
               !DX5  %in% icd9 & 
               !DX6  %in% icd9 & 
               !DX7  %in% icd9 & 
               !DX8  %in% icd9 & 
               !DX9  %in% icd9 & 
               !DX10 %in% icd9 & 
               !DX11 %in% icd9 & 
               !DX12 %in% icd9 & 
               !DX13 %in% icd9 & 
               !DX14 %in% icd9 & 
               !DX15 %in% icd9)
  }
}

exc_by_pr <- function(df, icd9) {
  filter(df, !PR1  %in% icd9 & 
             !PR2  %in% icd9 & 
             !PR3  %in% icd9 & 
             !PR4  %in% icd9 & 
             !PR5  %in% icd9 & 
             !PR6  %in% icd9 & 
             !PR7  %in% icd9 & 
             !PR8  %in% icd9 & 
             !PR9  %in% icd9 & 
             !PR10 %in% icd9 & 
             !PR11 %in% icd9 & 
             !PR12 %in% icd9 & 
             !PR13 %in% icd9 & 
             !PR14 %in% icd9 & 
             !PR15 %in% icd9)
}
