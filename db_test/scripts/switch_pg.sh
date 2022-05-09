build=`echo $PATH | grep pg_install | sed 's/.*pg_install\/\([0-9a-zA-Z_-]*\)\/.*/\1/'`
if [ x$build = x ]; then
	echo "current pg build not set"
else
	echo "current pg build = $build"
fi


if [ x$1 != x'-n' ]; then
	if [ \( -f /home/zyzhao/.last_pg_version \) -a \( \( x$1 = x'-o' \) -o \( x$build = x \) \) ]; then
		alt_build=`cat /home/zyzhao/.last_pg_version`
		echo "switching to the last set build $alt_build"
	else
		if [ $# -ge 1 ]; then
			alt_build="$1"
		else
			if [ x$build = x'release' ]; then
				alt_build='debug'
			elif [ x$build = x'debug' ]; then
				alt_build='release'
			else
				# defaults to release if not set (ever)
				alt_build='release'
			fi
		fi
		echo "switching to $alt_build"
	fi

	
	if [ x$build = x ]; then
		export PATH="/home/zyzhao/pg_install/$alt_build/bin:$PATH"
	else
		export PATH=`echo "$PATH" | sed "s,/home/zyzhao/pg_install/$build/bin,/home/zyzhao/pg_install/$alt_build/bin,"`
	fi

	export PGDATA="/mnt/ssd3/zyzhao/pg_data/$alt_build"

	echo $alt_build > /home/zyzhao/.last_pg_version
fi

