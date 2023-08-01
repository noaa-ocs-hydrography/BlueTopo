from argparse import ArgumentParser
from nbs.bluetopo.build_vrt import main as vrt
from nbs.bluetopo.fetch_tiles import main as fetch


def build_vrt_command():
    """
    console_scripts entry point for build_vrt cli command 

    """
    parser = ArgumentParser()
    parser.add_argument('-d', '--dir', '--directory', 
                        help='The directory path to use. ' 
                        'Will create if it does not currently exist. Required argument.', 
                        type=str, 
                        nargs='?',
                        dest ='dir',
                        required=True)
    parser.add_argument('-t', '--targ', '--target', 
                        help=('The NBS offers various products to different end-users. '
                        'Some are available publicly. Use this argument to identify '
                        'which product you want to target. BlueTopo is the default.'), 
                        type=str.lower, 
                        choices=["bluetopo", "modeling"], 
                        default='bluetopo',
                        dest='target',
                        nargs='?')
    args = parser.parse_args()
    vrt(root = args.dir, target = args.target)


def fetch_tiles_command():
    """
    console_scripts entry point for fetch_tiles cli command 

    """
    parser = ArgumentParser()
    parser.add_argument('-d', '--dir', '--directory', 
                        help='The directory path to use. ' 
                        'Will create if it does not currently exist. Required argument.', 
                        type=str, 
                        nargs='?',
                        dest='dir',
                        required=True)
    parser.add_argument('-g', '--geom', '--geometry', 
                        help=('The geometry file to use to find intersecting available tiles. '
                        'The returned tile ids at the time of intersection will be added to '
                        'tracking. fetch_tiles will stay up to date with the latest data '
                        'available from the NBS for all tracked tiles. This argument is '
                        'not necessary if you do not want to add new tile ids to tracking.'), 
                        type=str, 
                        dest='geom',
                        nargs='?')
    parser.add_argument('-t', '--targ', '--target', 
                        help=('The NBS offers various products to different end-users. '
                        'Some are available publicly. Use this argument to identify '
                        'which product you want to target. BlueTopo is the default.'), 
                        type=str.lower, 
                        choices=["bluetopo", "modeling"], 
                        default='bluetopo', 
                        dest='target',
                        nargs='?')
    parser.add_argument('-u', '--untrack', 
                        help=('This flag will untrack tiles that have missing files in your local '
                        'download directory. fetch_tiles will no longer retrieve these tiles.'), 
                        dest='untrack',
                        action="store_true")
    args = parser.parse_args()
    fetch(root = args.dir, desired_area_filename = args.geom, 
         untrack_missing = args.untrack, target = args.target)