/*
 * Copyright 2019 Google
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Firestore/core/src/firebase/firestore/core/transaction_runner.h"
#include "Firestore/core/src/firebase/firestore/remote/exponential_backoff.h"

#include <utility>

#include "absl/algorithm/container.h"

using firebase::firestore::core::TransactionResultCallback;
using firebase::firestore::core::TransactionUpdateCallback;
using firebase::firestore::remote::Datastore;
using firebase::firestore::remote::RemoteStore;
using firebase::firestore::util::AsyncQueue;
using firebase::firestore::util::TimerId;
using firebase::firestore::util::Status;

namespace firebase {
namespace firestore {
namespace core {
  TransactionRunner::TransactionRunner(const std::shared_ptr<AsyncQueue>& queue,
                    RemoteStore* remote_store,
                    TransactionUpdateCallback update_callback,
                    TransactionResultCallback result_callback)
  : queue_{queue},
  remote_store_{remote_store},
  update_callback_{update_callback},
  result_callback_{result_callback},
  backoff_{queue_, TimerId::RetryTransaction} {
    retries_left_ = kRetryCount;
  }

  void TransactionRunner::Run() {
    queue_->VerifyIsCurrentQueue();
    backoff_.BackoffAndRun([&] {
      std::shared_ptr<Transaction> transaction = remote_store_->CreateTransaction();
      update_callback_(transaction, [=](util::StatusOr<absl::any> maybe_result) {
        queue_->Enqueue([&] {
          if (!maybe_result.ok()) {
            if (retries_left_ > 0 && TransactionRunner::IsRetryableTransactionError(maybe_result.status()) &&
                !transaction->IsPermanentlyFailed()) {
              retries_left_ -= 1;
              TransactionRunner::Run();
            } else {
              result_callback_(std::move(maybe_result));
            }
          } else {
            transaction->Commit([&](Status status) {
              if (status.ok()) {
                result_callback_(std::move(maybe_result));
                return;
              }
              if (retries_left_ > 0 && TransactionRunner::IsRetryableTransactionError(maybe_result.status())&&
                  !transaction->IsPermanentlyFailed()) {
                queue_->VerifyIsCurrentQueue();
                retries_left_ -= 1;
                TransactionRunner::Run();
              }  else {
                result_callback_(std::move(status));
              }
            });
          }
        });
      });
    });
  }

  bool TransactionRunner::IsRetryableTransactionError(const util::Status& error) {
    // In transactions, the backend will fail outdated reads with FAILED_PRECONDITION and
    // non-matching document versions with ABORTED. These errors should be retried.
    Error code = error.code();
    return code == Error::Aborted || code == Error::FailedPrecondition ||
    !Datastore::IsPermanentError(error);
  }
}  // namespace core
}  // namespace firestore
}  // namespace firebase
