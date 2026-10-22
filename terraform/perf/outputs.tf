# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

output "private_ip" {
  description = "Each device's private address: what the devices use to reach each other."
  value       = { for name, d in google_compute_instance.device : name => d.network_interface[0].network_ip }
}

output "instance_name" {
  description = "Each device's instance name, for gcloud compute ssh."
  value       = { for name, d in google_compute_instance.device : name => d.name }
}

output "results_bucket" {
  description = "The bucket the run's checkpoints are copied to; null for the seed job."
  value       = one(google_storage_bucket.results[*].name)
}
