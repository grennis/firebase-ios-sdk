/*
 * Copyright 2018 Google
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

#import <UIKit/UIKit.h>

#import <FirebaseInAppMessaging/FIRInAppMessagingRendering.h>
#import "FIDTimeFetcher.h"

@protocol FIRInAppMessagingDisplayDelegate;

NS_ASSUME_NONNULL_BEGIN
@interface FIDBaseRenderingViewController : UIViewController
@property(nonatomic, readwrite) id<FIDTimeFetcher> timeFetcher;

@property(nonatomic, readwrite) id<FIRInAppMessagingDisplayDelegate> displayDelegate;

// These are the two methods we use to respond to app state change for the purpose of
// actual display time tracking. Subclass can override this one to have more logic for responding
// to the two events, but remember to trigger super's implementation.
- (void)appDidBecomeInactive:(UIApplication *)application;
- (void)appDidBecomeActive:(UIApplication *)application;

// Tracking the aggregate impression time for the rendered message. Used to determine when
// we are eaching the minimal iimpression time requirements. Exposed so that sub banner vc
// class can use it for auto dismiss tracking
@property(nonatomic) double aggregateImpressionTimeInSeconds;

// Call this when the user choose to dismiss the message
- (void)dismissView:(FIRInAppMessagingDismissType)dismissType;

// Call this when end user wants to follow the action url
- (void)followActionURL;

// Returns the in-app message being displayed. Overridden by message type subclasses.
- (FIRInAppMessagingDisplayMessage *)inAppMessage;

@end
NS_ASSUME_NONNULL_END
