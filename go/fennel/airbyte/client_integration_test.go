//go:build airbyte_integration

package airbyte

const AIRBYTE_SERVER_LOCAL = "http://localhost:8002/api"

// These are actual passwords for the test accounts.
func TestAirbyteSourceClient(t *testing.T) {
	client, err := NewClient(AIRBYTE_SERVER_LOCAL, "airbyte_topic")
	assert.NoError(t, err)
	s3Source := data_integration.S3{}
	s3Source.Name = "unit_test_s3_try5"
	s3Source.Bucket = "aditya-temp"
	s3Source.PathPrefix = "movie_lens"
	s3Source.AWSAccessKeyId = "AKIAQOLFGTNXJY2MZWLPA"
	s3Source.AWSSecretAccessKey = "8YCvIs8f0+7uPEJRK2mq164v9hNjOIIi3q1uV8rva"
	s3Source.Format = "csv"
	_, err := client.CreateSource(s3Source)
	assert.NoError(t, err)
	bigQuerySource := data_integration.BigQuery{}
	bigQuerySource.Name = "unit_test_big_query_try5"
	bigQuerySource.ProjectId = "gold-cocoa-356105"
	bigQuerySource.DatasetId = "gold-cocoa-356105.aditya_movie_tags"
	bigQuerySource.CredentialsJson = `
	{
	  "type": "service_account",
	  "project_id": "gold-cocoa-356105",
	  "private_key_id": "4b39629c285054f48ed1210027cc9309d0258992",
	  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDF3axRWkM5EW/1\nRlI3EGmoqbZarcdpONYx0k3FkeObSqAw5F5YEiekVqFqGnFriTBy+x2I2+FvH5ff\niM2hT+N3BF4ZlpFX4NYfhU5ET1HShJCZGAg+soADvhNJQ09k6JZbbUICEFu+ac7L\nFL9Jv+utHkgAJMavi56AhIlxLdMFgViBjWhAoc/puVI/VdWOIRH7kgxmnHUq7yUl\nfltoOjw2thSWEbGy95O4PmkONpCLRkh5+c9Z48sTgfUXwcnX7epbN/PHVnmeHgEL\nbtdNLiAye/w7VyMy2YqrxUm1HmPAINE3+tXG2cJ9I/dGyaS0JOcBIYDqX+2l+9V0\n6t1wM61fAgMBAAECggEACcuuJr7luhMLbGSlWFWHLZLtVLLqvuJR9Fh5gjD1SDBb\nGuCLbSfE76VcS2DwwYair+aPsUFVeUdioO3oZDaMx80fDXw/SM74OspCNC6LGfV3\nJSUj71qt7MGBuexLp26+VttjW6/MovIhCzvFNpT6SXFI1jz72x/54lntZf2DR2/o\nA53AE0ISQ0OYF+muaPX51DQV7akktlgdMJ6SIN3q/opEMRYRm+exuPWsqZYv3Mkv\nj4HkR8ykVgmYD/CrWtAQ4EvhPVoUh47IXNLqv1MvTHwJEBwZr/ENWZUPxe5+9jqj\n37zROWCB63g72gkxGTGCdGc+DjfXABo1KY6AYUpVLQKBgQDzlg9sWSCruls10HaI\n9/+nEZ5tuLwGY0/NcXlXq2wijXBu8haFuPIIHT42D3bV7wj8CoBozQPmVE13GGxX\n/Qv9ZHYmSJIH0RiupK45lVOAJGFpQzsFiT7sRwsVl2D76Qcz6kV6JW2VLEcStLNW\n6be6d/I7dRykTsdpW4YkD7oBXQKBgQDP8x/0zWpgUmlnyODuPzpoLXWuPjk/cGi9\nO43P+3NBMd8iUYvxZLa2HzXiBTD/dIITDtJ1htg6soHZNM/CnQdt0q2j5Shsdst+\nEM4kbPDx50HdKm/ZA4jDgbEP8JYUvOVznPN/M0ulShCqyuTII3WvDEQa2orz0Ipx\n4LBbf75R6wKBgQDxvxIbg/Eqvc7b/8JEeWyeUJwdZOQmqtV+nYnB+n1dZWYaleTI\nXh7G8s7VNE/KTmtqSLncSOv/4dbnxbNxN9B4cQXZRNi5LUoDuai3uX2fhXuvzmt7\nORTbxj439X7pRAJEAdNmQFMbY1A+Plxme5o+U+ByJe6BGzZJV+4vR/RgeQKBgCAo\nkh2SksvYktJo/1f40TiBJzzOBJ5p7Niu2Ax872L6qm4tPD4VfCgIBZYxhVVMGD2I\nQkXIl7HkHy6O+z42eIqkVRQOgUTczjVtteNuMYjHYakpQejGoiTR7qbvZtZILBfI\nAuP988nY/WDcRaspyK5McE/S0kBIVNCtlbhgtfcHAoGBAJz76T9+vo162VK/7ecq\nbDDGyiymo2hU6ix2vautM+BLZMPDaDk+bukknXo4bYo2pwVYD+vyc/XQ2npkN1S2\nS5rQ+xxOrJrckMQXR/o+canbXFt6BZ64owKsbnC+Bt+3zL2Y5XzW+MriFZrGXC38\n0jQrHpzUS9E8DQAXt5lw0zy3\n-----END PRIVATE KEY-----\n",
	  "client_email": "airbyte@gold-cocoa-356105.iam.gserviceaccount.com",
	  "client_id": "103688494615927672951",
	  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
	  "token_uri": "https://oauth2.googleapis.com/token",
	  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
	  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/airbyte%40gold-cocoa-356105.iam.gserviceaccount.com"
	}
	`
	_, err = client.CreateSource(bigQuerySource)
	assert.NoError(t, err)
}
