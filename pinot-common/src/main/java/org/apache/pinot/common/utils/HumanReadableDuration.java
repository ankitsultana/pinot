/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * TODO: Taken as is from Palantir: <a href="https://github.com/palantir/human-readable-types/blob/develop/human-readable-types/src/main/java/com/palantir/humanreadabletypes/HumanReadableDuration.java">...</a>
 */
public class HumanReadableDuration implements Serializable, Comparable<HumanReadableDuration> {
  /**
   * Serialization version.
   */
  private static final long serialVersionUID = 4215573187492209716L;

  /**
   * The pattern for parsing duration strings.
   */
  private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+)\\s*(\\S+)");

  private static final Map<String, TimeUnit> SUFFIXES = createSuffixes();

  private static Map<String, TimeUnit> createSuffixes() {
    Map<String, TimeUnit> suffixes = new HashMap<>();
    suffixes.put("ns", TimeUnit.NANOSECONDS);
    suffixes.put("nanosecond", TimeUnit.NANOSECONDS);
    suffixes.put("nanoseconds", TimeUnit.NANOSECONDS);
    suffixes.put("us", TimeUnit.MICROSECONDS);
    suffixes.put("microsecond", TimeUnit.MICROSECONDS);
    suffixes.put("microseconds", TimeUnit.MICROSECONDS);
    suffixes.put("ms", TimeUnit.MILLISECONDS);
    suffixes.put("millisecond", TimeUnit.MILLISECONDS);
    suffixes.put("milliseconds", TimeUnit.MILLISECONDS);
    suffixes.put("s", TimeUnit.SECONDS);
    suffixes.put("second", TimeUnit.SECONDS);
    suffixes.put("seconds", TimeUnit.SECONDS);
    suffixes.put("m", TimeUnit.MINUTES);
    suffixes.put("minute", TimeUnit.MINUTES);
    suffixes.put("minutes", TimeUnit.MINUTES);
    suffixes.put("h", TimeUnit.HOURS);
    suffixes.put("hour", TimeUnit.HOURS);
    suffixes.put("hours", TimeUnit.HOURS);
    suffixes.put("d", TimeUnit.DAYS);
    suffixes.put("day", TimeUnit.DAYS);
    suffixes.put("days", TimeUnit.DAYS);
    return suffixes;
  }

  private final long _count;
  private final TimeUnit _unit;

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#NANOSECONDS}.
   *
   * @param count the number of nanoseconds
   */
  public static HumanReadableDuration nanoseconds(long count) {
    return new HumanReadableDuration(count, TimeUnit.NANOSECONDS);
  }

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#MICROSECONDS}.
   *
   * @param count the number of microseconds
   */
  public static HumanReadableDuration microseconds(long count) {
    return new HumanReadableDuration(count, TimeUnit.MICROSECONDS);
  }

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#MILLISECONDS}.
   *
   * @param count the number of milliseconds
   */
  public static HumanReadableDuration milliseconds(long count) {
    return new HumanReadableDuration(count, TimeUnit.MILLISECONDS);
  }

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#SECONDS}.
   *
   * @param count the number of seconds
   */
  public static HumanReadableDuration seconds(long count) {
    return new HumanReadableDuration(count, TimeUnit.SECONDS);
  }

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#MINUTES}.
   *
   * @param count the number of minutes
   */
  public static HumanReadableDuration minutes(long count) {
    return new HumanReadableDuration(count, TimeUnit.MINUTES);
  }

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#HOURS}.
   *
   * @param count the number of hours
   */
  public static HumanReadableDuration hours(long count) {
    return new HumanReadableDuration(count, TimeUnit.HOURS);
  }

  /**
   * Obtains a new {@link HumanReadableDuration} using {@link TimeUnit#DAYS}.
   *
   * @param count the number of days
   */
  public static HumanReadableDuration days(long count) {
    return new HumanReadableDuration(count, TimeUnit.DAYS);
  }

  public static HumanReadableDuration valueOf(String duration) {
    final Matcher matcher = DURATION_PATTERN.matcher(duration);
    Preconditions.checkArgument(matcher.matches(), "Invalid duration: " + duration);
    final long count = Long.parseLong(matcher.group(1));
    final TimeUnit unit = SUFFIXES.get(matcher.group(2));
    if (unit == null) {
      throw new RuntimeException("Invalid duration. Wrong time unit: " + duration);
    }
    return new HumanReadableDuration(count, unit);
  }

  private HumanReadableDuration(long count, TimeUnit unit) {
    _count = count;
    _unit = Preconditions.checkNotNull(unit, "unit must not be null");
  }

  /**
   * The quantity of this duration in {@link TimeUnit time units}.
   */
  public long getQuantity() {
    return _count;
  }

  /**
   * The {@link TimeUnit time unit} of this duration.
   */
  public TimeUnit getUnit() {
    return _unit;
  }

  /**
   * Converts this duration to the total length in nanoseconds expressed as a {@code long}.
   */
  public long toNanoseconds() {
    return TimeUnit.NANOSECONDS.convert(_count, _unit);
  }

  /**
   * Converts this duration to the total length in microseconds expressed as a {@code long}.
   */
  public long toMicroseconds() {
    return TimeUnit.MICROSECONDS.convert(_count, _unit);
  }

  /**
   * Converts this duration to the total length in milliseconds expressed as a {@code long}.
   */
  public long toMilliseconds() {
    return TimeUnit.MILLISECONDS.convert(_count, _unit);
  }

  /**
   * Converts this duration to the total length in seconds expressed as a {@code long}.
   */
  public long toSeconds() {
    return TimeUnit.SECONDS.convert(_count, _unit);
  }

  /**
   * Converts this duration to the total length in minutes expressed as a {@code long}.
   */
  public long toMinutes() {
    return TimeUnit.MINUTES.convert(_count, _unit);
  }

  /**
   * Converts this duration to the total length in hours expressed as a {@code long}.
   */
  public long toHours() {
    return TimeUnit.HOURS.convert(_count, _unit);
  }

  /**
   * Converts this duration to the total length in days expressed as a {@code long}.
   */
  public long toDays() {
    return TimeUnit.DAYS.convert(_count, _unit);
  }

  /**
   * Converts this duration to an equivalent {@link Duration}.
   */
  public Duration toJavaDuration() {
    return Duration.of(_count, chronoUnit(_unit));
  }

  /**
   * Converts a {@code TimeUnit} to a {@code ChronoUnit}.
   * <p>
   * This handles the seven units declared in {@code TimeUnit}.
   *
   * @implNote This method can be removed in JDK9
   * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8141452">JDK-8141452</a>
   * @param unit the unit to convert, not null
   * @return the converted unit, not null
   */
  private static ChronoUnit chronoUnit(TimeUnit unit) {
    Preconditions.checkNotNull(unit, "unit");
    switch (unit) {
      case NANOSECONDS:
        return ChronoUnit.NANOS;
      case MICROSECONDS:
        return ChronoUnit.MICROS;
      case MILLISECONDS:
        return ChronoUnit.MILLIS;
      case SECONDS:
        return ChronoUnit.SECONDS;
      case MINUTES:
        return ChronoUnit.MINUTES;
      case HOURS:
        return ChronoUnit.HOURS;
      case DAYS:
        return ChronoUnit.DAYS;
      default:
        throw new IllegalArgumentException("Unknown TimeUnit constant: " + unit);
    }
  }

  /**
   * Compares this duration to the specified {@code HumanReadableDuration}.
   * <p>
   * The comparison is based on the total length of the durations.
   * It is "consistent with equals", as defined by {@link Comparable}.
   *
   * @param otherDuration  the other duration to compare to, not null
   * @return the comparator value, negative if less, positive if greater
   */
  @Override
  public int compareTo(HumanReadableDuration otherDuration) {
    if (_unit == otherDuration._unit) {
      return Long.compare(_count, otherDuration._count);
    }

    return Long.compare(toNanoseconds(), otherDuration.toNanoseconds());
  }

  /**
   * Checks if this duration is equal to the specified {@code HumanReadableDuration}.
   * <p>
   * The comparison is based on the total length of the durations.
   *
   * @param otherDuration  the other duration, null returns false
   * @return true if the other duration is equal to this one
   */
  @Override
  public boolean equals(Object otherDuration) {
    if (this == otherDuration) {
      return true;
    }
    if ((otherDuration == null) || (getClass() != otherDuration.getClass())) {
      return false;
    }
    final HumanReadableDuration duration = (HumanReadableDuration) otherDuration;
    if (_unit == duration._unit) {
      return _count == duration._count;
    }
    return toJavaDuration().equals(duration.toJavaDuration());
  }

  /**
   * A hash code for this duration.
   *
   * @return a suitable hash code
   */
  @Override
  public int hashCode() {
    return toJavaDuration().hashCode();
  }

  /**
   * A human-readable string representation of this duration.
   *
   * @return a human-readable string representation of this duration, not null
   */
  @Override
  @JsonValue
  public String toString() {
    String units = _unit.toString().toLowerCase(Locale.ENGLISH);
    if (_count == 1) {
      units = units.substring(0, units.length() - 1);
    }
    return Long.toString(_count) + ' ' + units;
  }
}