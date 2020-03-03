use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use rand::{thread_rng, Rng};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use itertools::Itertools;

#[derive(Clone)]
pub struct ScanSampling<Id: IdType> {
    pub base_scan: BaseScan<Id>,
    pub edges_queue: Vec<Vec<Id>>,
}

static mut FLAG: bool = false;

impl<Id: IdType> ScanSampling<Id> {
    pub fn new(out_subgraph: QueryGraph) -> ScanSampling<Id> {
        Self {
            base_scan: BaseScan::new(out_subgraph),
            edges_queue: vec![],
        }
    }
    pub fn set_edge_indices_to_sample(&mut self, edges: Vec<Id>, num_edges_to_sample: usize) {
        let mut rng = thread_rng();
        let num_edges = edges.len() / 2;
        if unsafe { FLAG == true } {
            let vec = vec![60, 88, 49, 47, 35, 83, 71, 21, 69, 74, 77, 17, 83, 82, 35, 14, 4, 15, 41, 20, 13, 18, 74, 77, 62, 10, 33, 52, 72, 53, 65, 65, 77, 47, 32, 2, 15, 83, 58, 15, 20, 25, 35, 20, 28, 21, 64, 16, 82, 42, 49, 57, 86, 66, 87, 16, 88, 17, 3, 7, 17, 88, 25, 78, 48, 87, 61, 8, 88, 18, 38, 22, 17, 11, 26, 60, 77, 72, 85, 10, 7, 33, 27, 49, 45, 72, 79, 43, 21, 61, 240, 28, 189, 287, 315, 93, 91, 1, 159, 54, 277, 37, 273, 102, 215, 44, 4, 195, 121, 120, 263, 248, 44, 187, 12, 220, 143, 282, 132, 303, 225, 125, 37, 307, 82, 182, 205, 293, 178, 75, 0, 215, 75, 290, 78, 171, 74, 136, 272, 302, 279, 157, 16, 206, 7, 96, 8, 57, 103, 147, 67, 148, 185, 138, 288, 277, 251, 278, 118, 318, 98, 72, 17, 311, 166, 100, 217, 262, 55, 100, 147, 173, 77, 229, 65, 32, 29, 243, 101, 161, 103, 300, 228, 314, 236, 83, 197, 22, 125, 51, 216, 125, 239, 173, 307, 107, 171, 97, 88, 203, 290, 245, 178, 21, 157, 197, 126, 1, 50, 231, 71, 294, 210, 25, 266, 244, 64, 202, 312, 10, 28, 193, 89, 245, 221, 110, 205, 262, 94, 139, 237, 271, 271, 303, 17, 58, 76, 111, 235, 261, 266, 113, 273, 157, 238, 78, 216, 278, 120, 35, 155, 5, 28, 309, 155, 315, 113, 190, 1, 286, 258, 152, 318, 101, 67, 69, 19, 111, 222, 281, 237, 1, 298, 261, 104, 283, 189, 192, 180, 53, 152, 275, 213, 91, 193, 2, 260, 34, 174, 18, 115, 262, 29, 188, 238, 290, 0, 236, 93, 202, 214, 54, 102, 8, 149, 22, 239, 114, 195, 290, 193, 100, 37, 270, 72, 32, 270, 283, 54, 62, 280, 286, 72, 145, 305, 233, 33, 88, 77, 280, 146, 222, 185, 186, 59, 303, 272, 217, 10, 55, 134, 296, 34, 248, 289, 8, 88, 295, 136, 246, 87, 164, 244, 149, 163, 213, 3, 212, 186, 94, 235, 124, 215, 126, 11, 217, 144, 137, 157, 194, 159, 62, 200, 198, 125, 132, 211, 180, 106, 198, 179, 71, 266, 232, 5, 44, 297, 36, 23, 242, 95, 262, 288, 141, 271, 256, 272, 263, 73, 298, 130, 68, 79, 218, 271, 240, 309, 157, 316, 135, 240, 28, 189, 287, 315, 93, 91, 1, 159, 54, 277, 37, 273, 102, 215, 44, 4, 195, 121, 120, 263, 248, 44, 187, 12, 220, 143, 282, 132, 303, 225, 125, 37, 307, 82, 182, 205, 293, 178, 75, 0, 215, 75, 290, 78, 171, 74, 136, 272, 302, 279, 157, 16, 206, 7, 96, 8, 57, 103, 147, 67, 148, 185, 138, 288, 277, 251, 278, 118, 318, 98, 72, 17, 311, 166, 100, 217, 262, 55, 100, 147, 173, 77, 229, 65, 32, 29, 243, 101, 161, 103, 300, 228, 314, 236, 83, 197, 22, 125, 51, 216, 125, 239, 173, 307, 107, 171, 97, 88, 203, 290, 245, 178, 21, 157, 197, 126, 1, 50, 231, 71, 294, 210, 25, 266, 244, 64, 202, 312, 10, 28, 193, 89, 245, 221, 110, 205, 262, 94, 139, 237, 271, 271, 303, 17, 58, 76, 111, 235, 261, 266, 113, 273, 157, 238, 78, 216, 278, 120, 35, 155, 5, 28, 309, 155, 315, 113, 190, 1, 286, 258, 152, 318, 101, 67, 69, 19, 111, 222, 281, 237, 1, 298, 261, 104, 283, 189, 192, 180, 53, 152, 275, 213, 91, 193, 2, 260, 34, 174, 18, 115, 262, 29, 188, 238, 290, 0, 236, 93, 202, 214, 54, 102, 8, 149, 22, 239, 114, 195, 290, 193, 100, 37, 270, 72, 32, 270, 283, 54, 62, 280, 286, 72, 145, 305, 233, 33, 88, 77, 280, 146, 222, 185, 186, 59, 303, 272, 217, 10, 55, 134, 296, 34, 248, 289, 8, 88, 295, 136, 246, 87, 164, 244, 149, 163, 213, 3, 212, 186, 94, 235, 124, 215, 126, 11, 217, 144, 137, 157, 194, 159, 62, 200, 198, 125, 132, 211, 180, 106, 198, 179, 71, 266, 232, 5, 44, 297, 36, 23, 242, 95, 262, 288, 141, 271, 256, 272, 263, 73, 298, 130, 68, 79, 218, 271, 240, 309, 157, 316, 135, 560, 188, 733, 383, 571, 317, 187, 129, 255, 790, 885, 357, 337, 70, 983, 716, 132, 611, 185, 952, 903, 440, 812, 699, 556, 764, 943, 890, 356, 975, 129, 637, 645, 915, 146, 278, 461, 165, 434, 491, 704, 503, 651, 546, 910, 107, 106, 424, 400, 846, 567, 829, 16, 910, 263, 608, 232, 793, 647, 787, 739, 308, 185, 234, 608, 629, 795, 886, 54, 638, 418, 840, 689, 663, 806, 324, 249, 550, 823, 356, 915, 621, 461, 5, 833, 512, 573, 819, 869, 865, 583, 108, 196, 762, 332, 723, 869, 278, 221, 19, 792, 605, 239, 13, 339, 971, 875, 865, 664, 779, 34, 821, 306, 117, 157, 357, 254, 897, 466, 583, 775, 710, 370, 249, 682, 20, 832, 650, 600, 170, 60, 673, 665, 661, 125, 302, 205, 518, 510, 139, 749, 975, 239, 367, 977, 26, 12, 175, 235, 773, 618, 785, 849, 93, 238, 846, 24, 118, 728, 163, 283, 5, 380, 85, 731, 347, 849, 126, 609, 190, 258, 536, 126, 581, 739, 901, 755, 207, 318, 537, 877, 673, 810, 837, 616, 795, 765, 224, 52, 981, 376, 947, 245, 955, 801, 610, 644, 802, 238, 402, 499, 294, 381, 540, 78, 354, 384, 780, 413, 362, 534, 54, 390, 232, 853, 342, 399, 498, 483, 674, 577, 4, 229, 526, 8, 160, 46, 603, 982, 702, 408, 798, 296, 113, 209, 649, 897, 248, 781, 760, 338, 382, 633, 858, 219, 335, 816, 857, 522, 759, 710, 840, 706, 536, 449, 488, 952, 423, 616, 118, 535, 420, 532, 821, 291, 437, 515, 980, 154, 446, 587, 796, 791, 990, 75, 57, 112, 521, 509, 322, 703, 926, 904, 70, 669, 548, 819, 404, 42, 966, 595, 647, 234, 968, 613, 108, 873, 740, 375, 146, 127, 454, 672, 429, 239, 0, 304, 295, 265, 554, 706, 356, 431, 666, 463, 912, 341, 829, 860, 807, 385, 911, 691, 128, 82, 668, 655, 311, 94, 307, 48, 150, 979, 548, 495, 255, 703, 406, 972, 568, 362, 679, 131, 788, 129, 193, 956, 890, 654, 405, 610, 820, 962, 983, 459, 86, 21, 758, 502, 41, 261, 854, 852, 211, 666, 716, 316, 389, 426, 213, 930, 577, 472, 820, 194, 837, 278, 838, 790, 504, 716, 835, 616, 688, 369, 915, 161, 894, 118, 970, 127, 663, 642, 578, 869, 767, 376, 659, 108, 212, 85, 178, 87, 949, 661, 323, 677, 429, 981, 892, 352, 351, 915, 408, 242, 175, 977, 501, 567, 875, 281, 619, 365, 373, 314, 265, 680, 366, 209, 813, 482, 320, 547, 31, 559, 263, 454, 330, 303, 30, 352, 313, 637, 386, 231, 234, 772, 709, 39, 210, 754, 881, 610, 45, 196, 470, 145, 671, 32, 882, 9, 524, 834, 540, 441, 337, 814, 973, 624, 829, 772, 940, 475, 431, 210, 757, 716, 365, 739, 421, 758, 734, 341, 68, 948, 229, 392, 47, 42, 890, 563, 289, 676, 103, 752, 221, 858, 284, 280, 891, 725, 975, 265, 876, 43, 947, 451, 396, 6, 884, 210, 964, 67, 493, 390, 248, 405, 166, 958, 595, 170, 813, 259, 941, 390, 527, 721, 407, 875, 435, 240, 515, 762, 724, 801, 242, 307, 25, 979, 955, 633, 627, 150, 309, 465, 306, 71, 866, 840, 164, 393, 568, 276, 108, 917, 652, 371, 634, 232, 733, 688, 460, 347, 282, 836, 769, 496, 466, 344, 630, 552, 311, 393, 447, 81, 845, 804, 3, 189, 84, 337, 468, 462, 207, 333, 810, 653, 945, 847, 700, 249, 804, 844, 538, 989, 693, 757, 137, 859, 927, 958, 668, 36, 594, 122, 587, 361, 416, 450, 179, 107, 580, 948, 874, 463, 335, 88, 284, 398, 144, 286, 359, 553, 255, 675, 592, 601, 647, 819, 513, 324, 430, 456, 767, 524, 962, 464, 651, 961, 961, 269, 21, 407, 901, 949, 856, 466, 170, 58, 887, 94, 541, 580, 978, 571, 836, 922, 964, 981, 484, 160, 860, 177, 658, 794, 981, 356, 410, 367, 532, 350, 453, 297, 415, 835, 843, 696, 693, 122, 629, 118, 557, 863, 109, 133, 750, 467, 516, 661, 360, 979, 800, 34, 238, 6, 673, 378, 145, 783, 846, 123, 442, 771, 288, 571, 195, 788, 586, 737, 434, 620, 35, 49, 627, 694, 179, 518, 721, 469, 790, 375, 625, 504, 143, 859, 44, 69, 268, 777, 486, 946, 50, 569, 47, 564, 293, 550, 982, 558, 566, 21, 805, 491, 266, 270, 381, 102, 482, 969, 655, 806, 492, 37, 563, 520, 262, 273, 873, 404, 938, 525, 330, 754, 634, 619, 810, 693, 649, 719, 684, 789, 68, 662, 769, 475, 539, 370, 831, 565, 112, 499, 315, 521, 123, 258, 962, 390, 503, 615, 153, 950, 333, 362, 335, 176, 155, 950, 28, 43, 96, 794, 601, 812, 902, 425, 59, 544, 941, 705, 77, 421, 944, 8, 713, 290, 23, 115, 722, 88, 632, 96, 721, 144, 266, 195, 11, 467, 871, 926, 699, 25, 61, 42, 785, 345, 370, 434, 742, 383, 370, 766, 123, 155, 599, 45, 589, 150, 243, 891, 535, 192, 604, 770, 517, 741, 436, 317, 272, 350, 628, 8, 730, 259, 501, 355, 360, 382, 603, 469, 151, 634, 863, 13, 380, 923, 279, 100, 669, 136, 836, 927, 325, 419, 407, 944, 328, 153, 640, 251, 915, 820, 53, 22, 814, 269, 220, 202, 434, 255, 181, 87, 161, 504, 135, 627, 198, 872, 69, 339, 426, 700, 989, 898, 656, 964, 925, 622, 494, 784, 418, 642, 840, 538, 702, 206, 970, 988, 869, 451, 738, 962, 757, 634, 216, 314, 927, 884, 300, 648, 726, 988, 586, 278, 947, 585, 235, 585, 611, 96, 459, 974, 781, 566, 515, 243, 484, 114, 675, 947, 805, 337, 974, 553, 749, 387, 721, 934, 507, 659, 353, 605, 189, 644, 176, 283, 218, 663, 12, 948, 987, 937, 980, 120, 855, 985, 184, 261, 601, 808, 428, 758, 368, 240, 268, 13, 461, 215, 425, 431, 471, 357, 380, 23, 323, 407, 172, 35, 194, 274, 249, 473, 38, 247, 126, 308, 41, 170, 46, 51, 160, 414, 377, 11, 245, 5, 155, 194, 398, 159, 65, 400, 285, 218, 205, 89, 272, 424, 93, 82, 106, 460, 294, 463, 345, 410, 282, 69, 196, 340, 89, 21, 367, 125, 340, 313, 150, 408, 87, 295, 278, 196, 0, 218, 472, 449, 407, 80, 330, 149, 162, 355, 64, 277, 249, 99, 157, 189, 468, 277, 133, 165, 133, 219, 178, 84, 210, 308, 229, 159, 12, 135, 135, 398, 279, 221, 311, 57, 139, 15, 213, 270, 193, 148, 263, 190, 187, 127, 349, 96, 235, 300, 447, 447, 398, 320, 91, 238, 194, 220, 482, 184, 282, 450, 383, 369, 257, 469, 436, 71, 212, 436, 117, 241, 407, 113, 191, 325, 52, 54, 383, 467, 119, 430, 271, 341, 257, 104, 204, 382, 38, 158, 321, 191, 477, 54, 473, 245, 35, 101, 184, 365, 88, 336, 184, 260, 243, 25, 199, 315, 391, 384, 19, 347, 357, 148, 179, 100, 41, 367, 38, 290, 361, 132, 49, 421, 203, 61, 116, 278, 100, 402, 58, 391, 182, 375, 86, 174, 360, 14, 46, 59, 250, 90, 308, 324, 456, 441, 284, 201, 352, 229, 410, 337, 446, 277, 466, 320, 162, 358, 195, 448, 464, 68, 148, 96, 399, 483, 263, 159, 390, 383, 390, 148, 74, 221, 158, 267, 237, 28, 263, 230, 101, 352, 120, 278, 230, 323, 20, 364, 109, 310, 108, 117, 0, 166, 87, 423, 151, 331, 300, 102, 162, 409, 392, 389, 92, 79, 275, 214, 77, 219, 448, 471, 306, 202, 472, 279, 382, 83, 64, 212, 276, 69, 17, 258, 162, 325, 224, 171, 340, 445, 94, 55, 330, 80, 383, 425, 424, 162, 415, 225, 210, 12, 340, 323, 258, 221, 360, 101, 249, 48, 431, 157, 193, 235, 346, 128, 302, 435, 175, 258, 277, 100, 312, 345, 110, 459, 231, 123, 220, 420, 354, 478, 415, 271, 176, 269, 63, 462, 294, 450, 269, 416, 262, 378, 27, 3, 72, 193, 70, 236, 97, 225, 78, 280, 93, 118, 406, 348, 177, 484, 479, 186, 391, 66, 438, 388, 135, 104, 316, 122, 98, 54, 87, 228, 382, 453, 407, 63, 76, 354, 154, 99, 403, 126, 322, 227, 311, 96, 129, 376, 248, 289, 198, 181, 445, 37, 189, 287, 173, 399, 168, 468, 319, 373, 142, 438, 429, 43, 345, 377, 101, 335, 171, 477, 255, 130, 65, 168, 182, 261, 129, 340, 446, 331, 123, 323, 277, 216, 182, 369, 456, 94, 167, 215, 268, 175, 178, 396, 73, 5, 336, 130, 263, 318, 469, 430, 166, 289, 183, 368, 52, 395, 140, 126, 226, 351, 431, 404, 385, 430, 341, 96, 78, 189, 297, 374, 51, 126, 385, 69, 339, 278, 402, 121, 312, 246, 195, 0, 28, 19, 17, 5, 23, 11, 21, 9, 14, 17, 17, 23, 22, 5, 14, 4, 15, 11, 20, 13, 18, 14, 17, 2, 10, 3, 22, 12, 23, 0, 28, 19, 17, 5, 23, 11, 21, 9, 14, 17, 17, 23, 22, 5, 14, 4, 15, 11, 20, 13, 18, 14, 17, 2, 10, 3, 22, 12, 23, 60, 88, 49, 47, 35, 83, 71, 21, 69, 74, 77, 17, 83, 82, 35, 14, 4, 15, 41, 20, 13, 18, 74, 77, 62, 10, 33, 52, 72, 53, 65, 65, 77, 47, 32, 2, 15, 83, 58, 15, 20, 25, 35, 20, 28, 21, 64, 16, 82, 42, 49, 57, 86, 66, 87, 16, 88, 17, 3, 7, 17, 88, 25, 78, 48, 87, 61, 8, 88, 18, 38, 22, 17, 11, 26, 60, 77, 72, 85, 10, 7, 33, 27, 49, 45, 72, 79, 43, 21, 61, 0, 0, 96, 44, 179, 177, 39, 35, 163, 15, 55, 16, 29, 23, 21, 2, 95, 0, 122, 175, 125, 154, 1, 80, 178, 151, 142, 58, 13, 180, 114, 69, 49, 73, 51, 49, 88, 136, 101, 175, 122, 15, 70, 61, 3, 84, 76, 141, 124, 56, 6, 42, 161, 167, 104, 68, 11, 66, 80, 109, 37, 9, 105, 82, 41, 118, 152, 83, 13, 96, 66, 38, 24, 0, 69, 59, 156, 118, 67, 150, 161, 66, 61, 111, 163, 29, 75, 4, 163, 49, 139, 53, 59, 48, 6, 44, 168, 77, 139, 136, 13, 37, 92, 65, 139, 73, 153, 129, 153, 25, 170, 129, 60, 123, 62, 75, 9, 79, 92, 155, 48, 133, 35, 142, 54, 157, 14, 148, 102, 172, 146, 88, 2, 145, 173, 17, 81, 80, 181, 178, 64, 69, 167, 5, 179, 105, 11, 58, 16, 93, 17, 145, 6, 95, 13, 97, 26, 42, 40, 92, 70, 99, 179, 41, 114, 165, 67, 9, 167, 104, 125, 174, 16, 80, 54, 43, 179, 109, 95, 43, 40, 37, 37, 163, 12, 16, 25, 23, 11, 35, 23, 15, 27, 2, 29, 23, 35, 16, 11, 14, 10, 21, 41, 14, 1, 24, 38, 11, 2, 16, 27, 40, 30, 41, 35, 17, 23, 35, 32, 38, 3, 35, 10, 15, 14, 19, 12, 2, 11, 9, 11, 7, 9, 1, 13, 2, 1, 9, 7, 2, 12, 2, 11, 9, 11, 7, 9, 1, 13, 2, 1, 9, 7, 2, 12, 16, 25, 23, 11, 35, 23, 15, 27, 2, 29, 23, 35, 16, 11, 14, 10, 21, 41, 14, 1, 24, 38, 11, 2, 16, 27, 40, 30, 41, 35, 17, 23, 35, 32, 38, 3, 35, 10, 15, 14, 19, 0, 4, 1, 5, 5, 5, 0, 1, 1, 0, 1, 1, 12, 2, 11, 9, 11, 7, 9, 1, 13, 2, 1, 9, 7, 2, 0, 1, 1, 0, 12, 2, 11, 9, 11, 7, 9, 1, 13, 2, 1, 9, 7, 2, 0, 1, 1, 0];
            for edge_idx in vec {
                self.edges_queue
                    .push(vec![edges[edge_idx], edges[edge_idx + 1]]);
            }
            unsafe {
                FLAG = false;
            }
        } else {
            while self.edges_queue.len() < num_edges_to_sample {
                let edge_idx = rng.gen_range(0, num_edges);
                print!("{},", edge_idx);
                self.edges_queue
                    .push(vec![edges[edge_idx], edges[edge_idx + 1]]);
            }
        }
    }

    pub fn set_edge_indices_to_sample_list(
        &mut self,
        edges: Vec<Vec<Id>>,
        num_edges_to_sample: usize,
    ) {
        let mut rng = thread_rng();
        while self.edges_queue.len() < num_edges_to_sample {
            let edge_idx = rng.gen_range(0, edges.len());
            self.edges_queue.push(edges[edge_idx].clone());
        }
    }

    pub fn set_edge_indices_to_sample_by_edges(
        &mut self,
        edges: Vec<Vec<Id>>,
        num_edges_to_sample: usize,
    ) {
        let mut rng = thread_rng();
        self.edges_queue = vec![vec![]; num_edges_to_sample];
        while self.edges_queue.len() < num_edges_to_sample {
            let edge_idx = rng.gen_range(0, edges.len());
            self.edges_queue.push(edges[edge_idx].clone());
        }
    }

    pub fn copy_default(&self) -> Operator<Id> {
        let mut scan_sampling = ScanSampling::new(self.base_scan.base_op.out_subgraph.clone());
        scan_sampling.edges_queue = self.edges_queue.clone();
        Operator::Scan(Scan::ScanSampling(scan_sampling))
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ScanSampling<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_scan.base_op.probe_tuple.is_empty() {
            self.base_scan.base_op.probe_tuple = probe_tuple.clone();
            self.base_scan.base_op.next.iter().for_each(|next_op| {
                next_op.borrow_mut().init(probe_tuple.clone(), graph);
            });
        }
    }

    fn process_new_tuple(&mut self) {
        self.base_scan.process_new_tuple()
    }

    fn execute(&mut self) {
        while !self.edges_queue.is_empty() {
            let edge = self.edges_queue.pop().unwrap();
            self.base_scan.base_op.probe_tuple[0] = edge[0];
            self.base_scan.base_op.probe_tuple[1] = edge[1];
            self.base_scan.base_op.num_out_tuples += 1;
            for next_op in &mut self.base_scan.base_op.next {
                next_op.borrow_mut().process_new_tuple();
            }
        }
    }

    fn get_alds_as_string(&self) -> String {
        self.base_scan.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_scan
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let mut scan_sampling = ScanSampling::new(self.base_scan.base_op.out_subgraph.clone());
        scan_sampling.edges_queue = self.edges_queue.clone();
        Operator::Scan(Scan::ScanSampling(scan_sampling))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        self.base_scan.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_scan.get_num_out_tuples()
    }
}
