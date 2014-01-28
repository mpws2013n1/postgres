select * from area, country_area, release_country, release where country_area.area = area.id and release_country.country=country_area.area and area.name = 'Laos';
