SELECT *
FROM "petstore"."pet_companions" pet_companions
JOIN "petstore"."pets" pets ON pet_companions."pet_id" = pets."id"
JOIN "petstore"."pets" companion_pet ON pet_companions."companion_pet_id" = companion_pet."id"
JOIN "petstore"."playgroups" playgroups ON pets."id" = playgroups."id"
JOIN "petstore"."vets" vets ON pets."id" = vets."id"
