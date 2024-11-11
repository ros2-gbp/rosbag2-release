%bcond_without tests
%bcond_without weak_deps

%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%global __provides_exclude_from ^/opt/ros/iron/.*$
%global __requires_exclude_from ^/opt/ros/iron/.*$

Name:           ros-iron-rosbag2-performance-benchmarking-msgs
Version:        0.22.8
Release:        1%{?dist}%{?release_suffix}
Summary:        ROS rosbag2_performance_benchmarking_msgs package

License:        Apache License 2.0
Source0:        %{name}-%{version}.tar.gz

Requires:       ros-iron-rosidl-default-runtime
Requires:       ros-iron-ros-workspace
BuildRequires:  ros-iron-ament-cmake
BuildRequires:  ros-iron-rosidl-default-generators
BuildRequires:  ros-iron-ros-workspace
BuildRequires:  ros-iron-rosidl-typesupport-fastrtps-c
BuildRequires:  ros-iron-rosidl-typesupport-fastrtps-cpp
Provides:       %{name}-devel = %{version}-%{release}
Provides:       %{name}-doc = %{version}-%{release}
Provides:       %{name}-runtime = %{version}-%{release}
Provides:       ros-iron-rosidl-interface-packages(member)

%if 0%{?with_tests}
BuildRequires:  ros-iron-ament-cmake-gtest
BuildRequires:  ros-iron-ament-lint-auto
BuildRequires:  ros-iron-ament-lint-common
BuildRequires:  ros-iron-rosidl-cmake
BuildRequires:  ros-iron-rosidl-typesupport-cpp
%endif

%if 0%{?with_weak_deps}
Supplements:    ros-iron-rosidl-interface-packages(all)
%endif

%description
A package containing rosbag2 performance benchmarking specific messages.

%prep
%autosetup -p1

%build
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/iron/setup.sh" ]; then . "/opt/ros/iron/setup.sh"; fi
mkdir -p .obj-%{_target_platform} && cd .obj-%{_target_platform}
%cmake3 \
    -UINCLUDE_INSTALL_DIR \
    -ULIB_INSTALL_DIR \
    -USYSCONF_INSTALL_DIR \
    -USHARE_INSTALL_PREFIX \
    -ULIB_SUFFIX \
    -DCMAKE_INSTALL_PREFIX="/opt/ros/iron" \
    -DAMENT_PREFIX_PATH="/opt/ros/iron" \
    -DCMAKE_PREFIX_PATH="/opt/ros/iron" \
    -DSETUPTOOLS_DEB_LAYOUT=OFF \
%if !0%{?with_tests}
    -DBUILD_TESTING=OFF \
%endif
    ..

%make_build

%install
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/iron/setup.sh" ]; then . "/opt/ros/iron/setup.sh"; fi
%make_install -C .obj-%{_target_platform}

%if 0%{?with_tests}
%check
# Look for a Makefile target with a name indicating that it runs tests
TEST_TARGET=$(%__make -qp -C .obj-%{_target_platform} | sed "s/^\(test\|check\):.*/\\1/;t f;d;:f;q0")
if [ -n "$TEST_TARGET" ]; then
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/iron/setup.sh" ]; then . "/opt/ros/iron/setup.sh"; fi
CTEST_OUTPUT_ON_FAILURE=1 \
    %make_build -C .obj-%{_target_platform} $TEST_TARGET || echo "RPM TESTS FAILED"
else echo "RPM TESTS SKIPPED"; fi
%endif

%files
/opt/ros/iron

%changelog
* Mon Nov 11 2024 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.8-1
- Autogenerated by Bloom

* Fri Jul 12 2024 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.7-1
- Autogenerated by Bloom

* Thu Feb 08 2024 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.6-1
- Autogenerated by Bloom

* Sat Nov 18 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.5-1
- Autogenerated by Bloom

* Wed Oct 25 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.4-1
- Autogenerated by Bloom

* Mon Sep 11 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.3-1
- Autogenerated by Bloom

* Fri Jul 14 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.2-1
- Autogenerated by Bloom

* Thu May 18 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.1-1
- Autogenerated by Bloom

* Thu Apr 20 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.0-2
- Autogenerated by Bloom

* Tue Apr 18 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.22.0-1
- Autogenerated by Bloom

* Thu Apr 13 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.21.0-2
- Autogenerated by Bloom

* Thu Apr 13 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.21.0-1
- Autogenerated by Bloom

* Tue Mar 21 2023 ROS Tooling Working Group <ros-tooling@googlegroups.com> - 0.20.0-2
- Autogenerated by Bloom

