mysql --version
echo mysql-apt-config mysql-apt-config/select-server select mysql-5.7 | sudo debconf-set-selections
wget https://dev.mysql.com/get/mysql-apt-config_0.8.6-1_all.deb
sudo dpkg --install mysql-apt-config_0.8.6-1_all.deb
sudo apt-get update -q
sudo apt-get install -q -y -o Dpkg::Options::=--force-confnew mysql-server
sudo mysql_upgrade --force
mysql --version
