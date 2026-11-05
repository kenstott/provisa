SELECT *
FROM "petstore"."pets" pets
JOIN "petstore"."pet_companions" pet_companions ON pets."id" = pet_companions."companion_pet_id"
JOIN "petstore"."pets" pet ON pet_companions."pet_id" = pet."id"
