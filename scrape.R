# Install required packages
install.packages("")

# Loading required packages
library(parallel)
library(rvest)
library(tidyverse)
library(xlsx)

# Cleaning environment
rm(list = ls())

# Function to obtain the number of products per category
get_total_products <- function(html) {
  html %>% 
    read_html() %>%
    html_nodes('.search-header_amountOfResults__fgvrB') %>%      
    html_text() %>%
    str_extract("[0-9]+") %>%
    as.numeric
}

# Function to obtain product name, price and quantity information
get_products <- function(html){
  
  product <- html %>% 
    read_html() %>%
    html_nodes('.title_lineclamp__1dS7X') %>%      
    html_text()
  
  price <-  html %>% 
    read_html() %>%
    html_nodes('.price_portrait__2B9Lk') %>% 
    html_text()
  
  result <- data.frame(product = product,
                       prijs = as.numeric(as.character(str_extract(price, "[0-9]+[.][0-9]{2}"))),
                       hoeveelheid = ifelse(!is.na(str_match(price, "[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{2}")), 
                                             yes = str_extract(price , "(?<=[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{2}).*"),
                                             no = str_extract(price , "(?<=[0-9]{1,3}[.][0-9]{2}).*")),
                       categorie = (str_extract(html, "(?<=https://www.ah.nl/producten/).*(?=\\?)")))
  return(result)
}

# Function to create required urls (required get_products function)
create_url <- function(html = "https://www.ah.nl/producten/") {

categories <- read_html(html) %>%
  html_nodes(".product-category-overview_category__1H99m") %>%
  html_text()  %>%
  str_replace_all(", ", "-") %>% 
  str_replace_all(" ", "-") %>%
  str_replace_all("ë", "e") %>%
  tolower()

url <- str_c(html, categories)
products <- as.numeric(sapply(url, get_total_products))
overview <- as.data.frame(cbind(categories, url, as.numeric(products)))
names(overview) <- c("afdeling", "url", "producten")
overview$producten <- as.numeric(overview$producten)

url1 <- str_c(overview$url[overview$producten <= 1000], "?sortBy=price&page=28")

url2 <- rep(overview$url[overview$producten > 1000 & overview$producten <= 2000], 4) %>%
        paste0(c(rep("?minPrice=0.00&maxPrice=1.50&sortBy=price&page=28", length(overview$url[overview$producten > 1000 & overview$producten <= 2000])),
                 rep("?minPrice=1.51&maxPrice=2.80&sortBy=price&page=28", length(overview$url[overview$producten > 1000 & overview$producten <= 2000])),
                 rep("?minPrice=2.81&maxPrice=5.00&sortBy=price&page=28", length(overview$url[overview$producten > 1000 & overview$producten <= 2000])),
                 rep("?minPrice=5.01&maxPrice=9999&sortBy=price&page=28", length(overview$url[overview$producten > 1000 & overview$producten <= 2000]))))

url3 <- rep(overview$url[overview$producten > 2000], 6) %>%
        paste0(c(rep("?minPrice=0.00&maxPrice=1.00&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=1.01&maxPrice=1.85&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=1.86&maxPrice=2.80&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=2.81&maxPrice=4.00&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=4.01&maxPrice=5.00&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=5.01&maxPrice=8.00&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=8.01&maxPrice=15.00&sortBy=price&page=28", length(overview$url[overview$producten > 2000])),
                 rep("?minPrice=15.01&maxPrice=9999&sortBy=price&page=28", length(overview$url[overview$producten > 2000]))))

url_list <- c(url1, url2, url3)

return(url_list)

}

# Scrape and merge data ---------------------------------------------------
urls <- create_url()

numCores <- detectCores()
cl <- makeCluster(numCores)

clusterEvalQ(cl, {
  varlist=c("urls")
  library(rvest)
  library(tidyverse)
})

system.time(
  datalist <- parLapply(cl, urls, get_products))

stopCluster(cl)

data <- bind_rows(datalist)

# Save scraped data
write.table(data, str_c(Sys.Date(), ".txt"), row.names = F)
write.csv(data, paste0(Sys.Date(), ".csv"), row.names = F)
write.xlsx2(data, paste0(Sys.Date(), ".xlsx"), row.names = F)


